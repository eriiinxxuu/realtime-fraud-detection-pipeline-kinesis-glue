output "ecs_task_execution_role_arn" { value = aws_iam_role.ecs_task_execution.arn }
output "ecs_task_role_arn"           { value = aws_iam_role.ecs_task.arn }
output "glue_role_arn"               { value = aws_iam_role.glue.arn }
output "sagemaker_role_arn"          { value = aws_iam_role.sagemaker.arn }
output "lambda_role_arn"             { value = aws_iam_role.lambda.arn }
output "redshift_role_arn"           { value = aws_iam_role.redshift.arn }
output "github_actions_role_arn"     { value = aws_iam_role.github_actions.arn }
