#!/usr/bin/env python

import os
from string import Template
import boto3

AWS_REGION_NAME = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

zappa_template = open('zappa_settings.template')
src = Template(zappa_template.read())

cfn = boto3.client('cloudformation', region_name=AWS_REGION_NAME)
CFN_OUTPUTS = {}

STACKS = (
    'neptune-dev-dev-vpc',
    'neptune-dev-dev-neptune',
    'neptune-dev-dev-etl',
)

for stack in STACKS:
    print 'Getting Outputs of stack: %s ...' % stack
    stack_details = cfn.describe_stacks(StackName=stack)
    for output in stack_details['Stacks'][0]['Outputs']:
        CFN_OUTPUTS[output['OutputKey']] = output['OutputValue']

print '\n\nOutputs captured: \n'
for key, value in CFN_OUTPUTS.items():
    print '%s: %s' % (key, value)

rendered = src.substitute(CFN_OUTPUTS)

print '\n\nGenerating dynamic zappa_settings.yml...'
zappa_settings = open('zappa_settings.yml', 'wb')
zappa_settings.write(rendered)
zappa_settings.close()


# Manually add S3 Loader IAM Role to Neptune Cluster as Neptune Cloudformation
# does not yet support IAM management

print '\n\nAdding IAM role [%s] to Neptune cluster [%s] ...' % (
    CFN_OUTPUTS['NeptuneLoadFromS3IAMRoleArn'],
    CFN_OUTPUTS['DBClusterId']
)
neptune = boto3.client('neptune', region_name=AWS_REGION_NAME)
neptune.add_role_to_db_cluster(
    DBClusterIdentifier=CFN_OUTPUTS['DBClusterId'],
    RoleArn=CFN_OUTPUTS['NeptuneLoadFromS3IAMRoleArn']
)

# Subscribing Lambda to SNS topic
sns = boto3.client('sns', region_name=AWS_REGION_NAME)
FUNCTION_ARN = 'arn:aws:lambda:{region_name}:{account_id}:function:{function_name}'.format(
    region_name=AWS_REGION_NAME,
    account_id=CFN_OUTPUTS['AWSAccountId'],
    function_name='neptune-event-event-dev'
)

sns.subscribe(
    TopicArn=CFN_OUTPUTS['GlueJobSucceededTopic'],
    Protocol='lambda',
    Endpoint=FUNCTION_ARN
)
