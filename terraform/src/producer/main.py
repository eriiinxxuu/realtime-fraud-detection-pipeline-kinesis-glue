from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import os
import random
import signal
import time
from typing import Optional
from zoneinfo import ZoneInfo
import geonamescache
from timezonefinder import TimezoneFinder
from dotenv import load_dotenv
from faker import Faker
from geopy.distance import geodesic
from jsonschema import ValidationError, validate, FormatChecker
import json
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)
load_dotenv(dotenv_path="/app/.env")

fake = Faker()


TRANSACTION_SCHEMA = {
    "type": "object",
    "required": [
        "transaction_id", "user_id", "amount", "currency", "timestamp",
        "merchant", "location", "device_id", "mcc", "is_fraud",
        "risk_signals", "risk_score", "user_profile_summary"
    ],
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "integer"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
        "timestamp": {"type": "string"},
        "merchant": {"type": "string"},
        "location": {"type": "string"},
        "device_id": {"type": "string"},
        "mcc": {"type": "string"},
        "is_fraud": {"type": "integer", "enum": [0, 1]},
        "fraud_type": {"type": ["string", "null"]},
        "fraud_details": {
            "type": ["object", "null"],
            "properties": {
                "fraud_subtype": {"type": "string"},
                "original_device": {"type": "string"},
                "compromised_via": {"type": "string"},
                "test_count": {"type": "integer"},
                "pattern": {"type": "string"},
                "merchant_risk_level": {"type": "string"},
                "mcc_category": {"type": "string"},
                "travel_speed_required": {"type": "string"},
                "legitimate_vpn_possible": {"type": "boolean"},
                "local_hour": {"type": "integer"},
                "user_night_rate": {"type": "number"},
                "transactions_last_hour": {"type": "integer"},
                "threshold_exceeded": {"type": "boolean"},
                "is_recurring_merchant": {"type": "boolean"},
                "previous_chargebacks": {"type": "integer"},
                "delivery_confirmed": {"type": "boolean"},
                "dispute_reason": {"type": "string"}
            }
        },
        "risk_signals": {
            "type": "array",
            "items": {"type": "string"}
        },
        "risk_score": {"type": "number", "minimum": 0, "maximum": 1},
        "user_profile_summary": {
            "type": "object",
            "required": ["account_age_days", "is_frequent_traveler", "avg_transaction", "home_country"],
            "properties": {
                "account_age_days": {"type": "integer"},
                "is_frequent_traveler": {"type": "boolean"},
                "avg_transaction": {"type": "number"},
                "home_country": {"type": "string"}
            }
        }
    },
    "if": {
        "properties": {"is_fraud": {"const": 1}}
    },
    "then": {
        "required": ["fraud_type", "fraud_details"],
        "properties": {
            "fraud_type": {"type": "string"},
            "fraud_details": {"type": "object"}
        }
    },
    "else": {
        "properties": {
            "fraud_type": {"const": None},
            "fraud_details": {"const": None}
        }
    }
}


class TransactionProducer:
    def __init__(self):
        # ── Kinesis config (replaces Kafka) ──────────────────────
        self.stream_name = os.getenv("KINESIS_STREAM_NAME", "fraud-detection-v2-transactions")
        self.aws_region  = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        self.running     = False

        self.kinesis_client = boto3.client("kinesis", region_name=self.aws_region)
        logger.info("Kinesis client initialized for stream: %s", self.stream_name)

        # ── Fraud rate control ────────────────────────────────────
        self.target_fraud_rate = 0.02
        self.fraud_count  = 0
        self.total_count  = 0

        self.compromised_users    = set(random.sample(range(1000, 10000), k=50))
        self.high_risk_merchants  = ['QuickCash', 'GlobalDigital', 'FastLoans',
                                     'MoneyNow', 'CashExpress', 'CryptoExchangePro']

        self.gc = geonamescache.GeonamesCache()
        self.tf = TimezoneFinder()
        self.capital_coords_cache  = {}
        self.country_timezones_cache = {}
        self.available_countries   = list(self.gc.get_countries().keys())
        self.user_profiles         = self._initialize_user_profiles()
        self.user_transaction_history  = defaultdict(lambda: deque(maxlen=100))
        self.user_chargeback_history   = defaultdict(list)
        self.high_risk_mcc = ['6211', '5962', '7995', '7801']

        signal.signal(signal.SIGINT,  self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info("Shutting down producer...")
            self.running = False
            logger.info("Producer closed.")

    def _get_capital_coordinates(self, country_code: str) -> Optional[tuple]:
        if country_code in self.capital_coords_cache:
            return self.capital_coords_cache[country_code]
        try:
            countries   = self.gc.get_countries()
            country_data = countries.get(country_code)
            if not country_data:
                return None
            capital = country_data.get('capital')
            if not capital:
                return None
            cities = self.gc.get_cities()
            for city_id, city_data in cities.items():
                if city_data['name'] == capital and city_data['countrycode'] == country_code:
                    coords = (city_data['latitude'], city_data['longitude'])
                    self.capital_coords_cache[country_code] = coords
                    return coords
            return None
        except Exception:
            return None

    def _get_country_timezone(self, country_code: str) -> str:
        if country_code in self.country_timezones_cache:
            return self.country_timezones_cache[country_code]
        try:
            coords = self._get_capital_coordinates(country_code)
            if not coords:
                self.country_timezones_cache[country_code] = 'UTC'
                return 'UTC'
            lat, lon = coords
            tz_name  = self.tf.timezone_at(lat=lat, lng=lon)
            result   = tz_name if tz_name else 'UTC'
            self.country_timezones_cache[country_code] = result
            return result
        except Exception as e:
            logger.error("Error getting timezone for %s: %s", country_code, e)
            self.country_timezones_cache[country_code] = 'UTC'
            return 'UTC'

    def _initialize_user_profiles(self) -> dict:
        profiles = {}
        for user_id in range(1000, 10000):
            home_country = random.choice(self.available_countries)
            avg_amount   = random.uniform(30, 300)
            profiles[user_id] = {
                'home_country':           home_country,
                'timezone':               self._get_country_timezone(home_country),
                'avg_transaction':        avg_amount,
                'typical_merchants':      [fake.company() for _ in range(5)],
                'night_transaction_rate': random.uniform(0.01, 0.15),
                'device_id':              f"dev_{user_id}_{random.randint(100, 999)}",
                'created_date':           datetime.now(timezone.utc) - timedelta(days=random.randint(30, 730)),
                'is_frequent_traveler':   random.random() < 0.08,
                'uses_vpn_regularly':     random.random() < 0.05,
                'has_secondary_residence': random.random() < 0.03,
                'secondary_country':      None,
                'chargeback_tendency':    random.random() < 0.02,
            }
            if profiles[user_id]['has_secondary_residence']:
                profiles[user_id]['secondary_country'] = random.choice(
                    [c for c in self.available_countries if c != home_country]
                )
        return profiles

    def _get_user_local_time(self, user_id: int, current_utc: datetime) -> datetime:
        profile = self.user_profiles[user_id]
        try:
            return current_utc.astimezone(ZoneInfo(profile['timezone']))
        except Exception:
            return current_utc

    def _get_user_local_hour(self, user_id: int, current_utc: datetime) -> int:
        return self._get_user_local_time(user_id, current_utc).hour

    def _calculate_distance(self, country1: str, country2: str) -> float:
        coords1 = self._get_capital_coordinates(country1)
        coords2 = self._get_capital_coordinates(country2)
        if not coords1 or not coords2:
            return 0
        return geodesic(coords1, coords2).kilometers

    def _check_impossible_travel(self, user_id: int, current_location: str,
                                 current_time: datetime) -> bool:
        history = self.user_transaction_history[user_id]
        if not history:
            return False
        last_txn      = history[-1]
        last_location = last_txn['location']
        last_time     = last_txn['timestamp']
        if last_location == current_location:
            return False
        time_diff = (current_time - last_time).total_seconds() / 3600
        distance  = self._calculate_distance(last_location, current_location)
        return time_diff < (distance / 900) and distance > 500

    def _count_recent_transactions(self, user_id: int, minutes: int) -> int:
        history     = self.user_transaction_history[user_id]
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        return sum(1 for txn in history if txn['timestamp'] > cutoff_time)

    def _is_legitimate_anomaly(self, user_id: int, transaction: dict,
                                current_time: datetime) -> bool:
        profile = self.user_profiles[user_id]
        if profile['is_frequent_traveler'] and transaction['location'] != profile['home_country']:
            return True
        if profile['has_secondary_residence'] and transaction['location'] == profile['secondary_country']:
            return True
        if profile['uses_vpn_regularly']:
            return True
        return False

    def _check_friendly_fraud(self, user_id: int, transaction: dict):
        profile = self.user_profiles[user_id]
        if not profile['chargeback_tendency']:
            return False, None
        chargeback_count = len(self.user_chargeback_history[user_id])
        fraud_probability = min(0.15 + chargeback_count * 0.05, 0.4)
        if random.random() < fraud_probability:
            if transaction['amount'] > profile['avg_transaction'] * 2:
                fraud_details = {
                    'fraud_subtype':           'friendly_fraud',
                    'is_recurring_merchant':   random.random() < 0.3,
                    'previous_chargebacks':    chargeback_count,
                    'delivery_confirmed':      random.random() < 0.8,
                    'dispute_reason':          random.choice([
                        'item_not_received', 'item_not_as_described',
                        'unauthorized_transaction', 'subscription_not_cancelled'
                    ])
                }
                self.user_chargeback_history[user_id].append({
                    'timestamp': datetime.now(timezone.utc),
                    'amount':    transaction['amount']
                })
                return True, fraud_details
        return False, None

    def _calculate_risk_signals(self, txn: dict, user_id: int,
                                 current_time: datetime) -> list:
        signals = []
        profile = self.user_profiles[user_id]
        if txn['amount'] > profile['avg_transaction'] * 5:
            signals.append('amount_anomaly_5x')
        elif txn['amount'] > profile['avg_transaction'] * 3:
            signals.append('amount_anomaly_3x')
        if txn['location'] != profile['home_country']:
            if not (profile['has_secondary_residence'] and
                    txn['location'] == profile['secondary_country']):
                signals.append('geo_anomaly')
        if txn['merchant'] in self.high_risk_merchants:
            signals.append('high_risk_merchant')
        if self._count_recent_transactions(user_id, 10) > 5:
            signals.append('velocity_10min_high')
        if self._count_recent_transactions(user_id, 60) > 15:
            signals.append('velocity_1h_high')
        if txn['device_id'] != profile['device_id']:
            signals.append('device_change')
        if 2 <= self._get_user_local_hour(user_id, current_time) <= 5:
            signals.append('night_transaction')
        if (current_time - profile['created_date']).days < 30:
            signals.append('new_account')
        if txn['mcc'] in self.high_risk_mcc:
            signals.append('high_risk_mcc')
        return signals

    def _calculate_risk_score(self, signals: list) -> float:
        weights = {
            'amount_anomaly_3x': 0.10, 'amount_anomaly_5x': 0.18,
            'geo_anomaly': 0.15,       'high_risk_merchant': 0.20,
            'velocity_10min_high': 0.15, 'velocity_1h_high': 0.12,
            'device_change': 0.20,     'night_transaction': 0.08,
            'new_account': 0.10,       'high_risk_mcc': 0.15,
        }
        score = sum(weights.get(s, 0.05) for s in signals) + random.uniform(-0.05, 0.05)
        return round(max(0, min(score, 1.0)), 3)

    def generate_transaction(self) -> Optional[dict]:
        self.total_count += 1
        user_id = random.randint(1000, 9999)
        profile = self.user_profiles[user_id]

        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id':        user_id,
            'amount':         max(0.01, round(random.gauss(profile['avg_transaction'],
                                                           profile['avg_transaction'] * 0.5), 2)),
            'currency':       'USD',
            'timestamp':      datetime.now(timezone.utc) + timedelta(seconds=random.randint(-300, 3000)),
            'merchant':       random.choice(profile['typical_merchants']),
            'location':       profile['home_country'],
            'device_id':      profile['device_id'],
            'mcc':            random.choice(['5411', '5812', '5999', '4814']),
            'is_fraud':       0,
            'fraud_type':     None,
            'fraud_details':  None,
        }

        is_fraud     = 0
        fraud_type   = None
        fraud_details = None
        current_time = transaction['timestamp']
        is_legit_anomaly = self._is_legitimate_anomaly(user_id, transaction, current_time)

        # 1. Account Takeover
        if user_id in self.compromised_users and not is_fraud:
            if random.random() < 0.35:
                is_fraud = 1
                fraud_type = 'account_takeover'
                transaction['amount']    = round(random.uniform(800, 5000), 2)
                transaction['device_id'] = f"dev_fraud_{random.randint(10000, 99999)}"
                if random.random() < 0.6:
                    foreign = [c for c in self.available_countries if c != profile['home_country']]
                    if foreign:
                        transaction['location'] = random.choice(foreign)
                if random.random() < 0.5:
                    transaction['merchant'] = random.choice(self.high_risk_merchants)
                    transaction['mcc']      = random.choice(self.high_risk_mcc)
                fraud_details = {
                    'fraud_subtype':   'account_takeover',
                    'original_device': profile['device_id'],
                    'compromised_via': random.choice(['phishing', 'credential_stuffing',
                                                      'malware', 'social_engineering'])
                }

        # 2. Card Testing
        if not is_fraud:
            recent_count = self._count_recent_transactions(user_id, minutes=10)
            if recent_count >= 5 and random.random() < 0.4:
                is_fraud = 1; fraud_type = 'card_testing'
                transaction['amount'] = round(random.uniform(0.50, 2.00), 2)
                fraud_details = {'fraud_subtype': 'card_testing', 'test_count': recent_count, 'pattern': 'rapid_small_amounts'}
            elif transaction['amount'] < 5 and random.random() < 0.005:
                is_fraud = 1; fraud_type = 'card_testing'
                transaction['amount']   = round(random.uniform(0.01, 1.99), 2)
                transaction['merchant'] = 'Charity Donation Portal'
                fraud_details = {'fraud_subtype': 'card_testing', 'test_count': 1, 'pattern': 'charity_test'}

        # 3. High-Risk Merchant
        if not is_fraud and random.random() < 0.04:
            transaction['merchant'] = random.choice(self.high_risk_merchants)
            transaction['mcc']      = random.choice(self.high_risk_mcc)
            if random.random() < 0.08:
                is_fraud = 1; fraud_type = 'high_risk_merchant'
                transaction['amount'] = round(random.uniform(500, 3000), 2)
                fraud_details = {'fraud_subtype': 'high_risk_merchant', 'merchant_risk_level': 'high', 'mcc_category': transaction['mcc']}

        # 4. Impossible Travel
        if not is_fraud and not is_legit_anomaly:
            if self._check_impossible_travel(user_id, transaction['location'], current_time):
                if random.random() < 0.6:
                    is_fraud = 1; fraud_type = 'impossible_travel'
                    transaction['device_id'] = f"dev_fraud_{random.randint(10000, 99999)}"
                    fraud_details = {'fraud_subtype': 'impossible_travel', 'travel_speed_required': 'supersonic', 'legitimate_vpn_possible': False}

        # 5. Unusual Hours
        if not is_fraud and not is_legit_anomaly:
            user_local_hour = self._get_user_local_hour(user_id, current_time)
            if 2 <= user_local_hour <= 5:
                if transaction['amount'] > profile['avg_transaction'] * 2 and profile['night_transaction_rate'] < 0.1:
                    if random.random() < 0.15:
                        is_fraud = 1; fraud_type = 'unusual_hours'
                        transaction['amount'] = round(random.uniform(1000, 4000), 2)
                        if random.random() < 0.4:
                            transaction['device_id'] = f"dev_fraud_{random.randint(10000, 99999)}"
                        fraud_details = {'fraud_subtype': 'unusual_hours', 'local_hour': user_local_hour, 'user_night_rate': profile['night_transaction_rate']}

        # 6. Velocity Abuse
        if not is_fraud:
            recent_1h = self._count_recent_transactions(user_id, minutes=60)
            if recent_1h >= 10 and random.random() < 0.3:
                is_fraud = 1; fraud_type = 'velocity_abuse'
                transaction['amount'] = round(random.uniform(50, 500), 2)
                fraud_details = {'fraud_subtype': 'velocity_abuse', 'transactions_last_hour': recent_1h, 'threshold_exceeded': True}

        # 7. Friendly Fraud
        if not is_fraud:
            is_friendly, friendly_details = self._check_friendly_fraud(user_id, transaction)
            if is_friendly:
                is_fraud = 1; fraud_type = 'friendly_fraud'; fraud_details = friendly_details

        # Noise
        if is_fraud and random.random() < 0.5:
            transaction['amount']    = round(random.gauss(profile['avg_transaction'], profile['avg_transaction'] * 0.3), 2)
            transaction['device_id'] = profile['device_id']
            transaction['location']  = profile['home_country']

        if not is_fraud and random.random() < 0.15:
            transaction['amount'] = round(random.uniform(500, 2000), 2)
            if random.random() < 0.4:
                transaction['merchant'] = random.choice(self.high_risk_merchants)
            if random.random() < 0.3:
                transaction['device_id'] = f"dev_new_{random.randint(10000, 99999)}"

        transaction['is_fraud']     = is_fraud
        transaction['fraud_type']   = fraud_type
        transaction['fraud_details'] = fraud_details
        transaction['risk_signals'] = self._calculate_risk_signals(transaction, user_id, current_time)
        transaction['risk_score']   = self._calculate_risk_score(transaction['risk_signals'])
        transaction['user_profile_summary'] = {
            'account_age_days':     (current_time - profile['created_date']).days,
            'is_frequent_traveler': profile['is_frequent_traveler'],
            'avg_transaction':      round(profile['avg_transaction'], 2),
            'home_country':         profile['home_country'],
        }

        if is_fraud:
            self.fraud_count += 1

        transaction_record = transaction.copy()
        transaction_record['timestamp'] = current_time
        self.user_transaction_history[user_id].append(transaction_record)
        transaction['timestamp'] = current_time.isoformat()

        if self.validate_transaction_schema(transaction):
            return transaction
        return None

    def validate_transaction_schema(self, transaction: dict) -> bool:
        try:
            validate(instance=transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())
            return True
        except ValidationError as e:
            logger.error("Invalid transaction: %s", e.message)
            return False

    def send_transaction(self) -> bool:
        """Generate and send a single transaction to Kinesis."""
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False

            self.kinesis_client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(transaction).encode("utf-8"),
                PartitionKey=str(transaction['user_id']),
            )
            logger.info("Sent transaction %s (fraud=%s)", transaction['transaction_id'], transaction['is_fraud'])
            return True

        except ClientError as e:
            logger.error("Failed to send to Kinesis: %s", e)
            return False

    def run_continuous_production(self, interval: float = 0.0):
        self.running = True
        logger.info("Starting Kinesis producer for stream: %s", self.stream_name)
        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
        finally:
            self.shutdown()



if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()