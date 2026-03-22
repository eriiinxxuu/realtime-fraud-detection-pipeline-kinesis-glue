output "vpc_id"               { value = aws_vpc.main.id }
output "private_subnet_ids"   { value = aws_subnet.private[*].id }
output "ecs_sg_id"            { value = aws_security_group.ecs.id }
output "lambda_sg_id"         { value = aws_security_group.lambda.id }
output "sagemaker_sg_id"      { value = aws_security_group.sagemaker.id }
output "redshift_sg_id"       { value = aws_security_group.redshift.id }
output "vpc_endpoints_sg_id"  { value = aws_security_group.vpc_endpoints.id }
output "private_rt_id"        { value = aws_route_table.private.id }