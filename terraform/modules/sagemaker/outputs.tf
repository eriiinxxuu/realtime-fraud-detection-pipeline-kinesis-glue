output "endpoint_name" { value = aws_sagemaker_endpoint.fraud.name }
output "endpoint_arn"  { value = aws_sagemaker_endpoint.fraud.arn }
output "model_name"    { value = aws_sagemaker_model.fraud.name }
