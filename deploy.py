from string import Template
import boto3

zappa_template = open('zappa_settings.template')
src = Template(zappa_template.read())

cfn = boto3.client('cloudformation', region_name='us-east-1')
REGISTRY = {}

STACKS = (
    'neptune-dev-dev-vpc',
    'neptune-dev-dev-neptune',
)

for stack in STACKS:
    print 'Getting Outputs of stack: %s ...' % stack
    stack_details = cfn.describe_stacks(StackName=stack)
    for output in stack_details['Stacks'][0]['Outputs']:
        REGISTRY[output['OutputKey']] = output['OutputValue']

print '\nOutputs captured: \n'
for key, value in REGISTRY.items():
    print '%s: %s' % (key, value)

rendered = src.substitute(REGISTRY)

print '\nGenerating zappa_settings.yml...'
zappa_settings = open('app/zappa_settings.yml', 'wb')
zappa_settings.write(rendered)
zappa_settings.close()
