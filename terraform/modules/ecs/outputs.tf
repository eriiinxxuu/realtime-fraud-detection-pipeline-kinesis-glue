output "cluster_id"           { value = aws_ecs_cluster.main.id }
output "cluster_name"         { value = aws_ecs_cluster.main.name }
output "producer_service_name" { value = aws_ecs_service.producer.name }
