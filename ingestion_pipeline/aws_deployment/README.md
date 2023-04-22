# Deployment to AWS

To integrate with other systems, this elastic search instance must be deployed to AWS. Using
the AWS CloudFormation template, the necessary resources can be created as follows:

```bash
aws cloudformation create-stack \
  --stack-name doyen-es \
  --template-body file:///full/path/to/doyen/ingestion_pipeline/aws_deployment/template.yaml \
  --parameters ParameterKey=MyHomeIp,ParameterValue=<your ip>
```

You can monitor the progress of the deployment using

```bash
aws cloudformation describe-stacks --stack-name doyen-es --query 'Stacks[0].StackStatus'
```

If there is an error, you can examine the logs using

```bash
aws cloudformation describe-stack-events --stack-name doyen-es
```
