import boto3

region = 'ap-southeast-2'
instances = ['']
ec2 = boto3.client('ec2', region_name=region)

def lambda_handler(event, context):
    ec2.start_instances(InstanceIds=instances)
    print('Started instances: ' + str(instances))

    return {
        'statusCode': 200,
    }