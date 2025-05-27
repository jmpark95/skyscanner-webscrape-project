import json
import boto3
from boto3.dynamodb.types import TypeDeserializer

dynamodb = boto3.resource('dynamodb')
user_table = dynamodb.Table('Users')
history_table = dynamodb.Table('History')
record_prices = dynamodb.Table('Record_Prices')

def ddb_deserialize(r, type_deserializer = TypeDeserializer()):
    return type_deserializer.deserialize({"M": r})

def lambda_handler(event, context):
    eventDeserialized = [ ddb_deserialize(r["dynamodb"]["NewImage"]) for r in event['Records'] ][0]
    user_id = eventDeserialized['user_id']
    new_user = eventDeserialized['new_user']
    date_scraped = eventDeserialized['date_scraped']
    todays_snapshot_depart_prices = eventDeserialized['prices']['depart_prices']
    todays_snapshot_return_prices = eventDeserialized['prices']['return_prices']


    if (new_user == 'N'):
        response = record_prices.get_item(Key={'user_id': user_id})
        record_depart_prices = response['Item']['prices']['depart_prices']
        record_return_prices = response['Item']['prices']['return_prices']

        # Compare today vs record. If today is cheaper, update
        record_broken_depart_prices = {}
        record_broken_return_prices = {}

        for day in record_depart_prices:
            if ( int(todays_snapshot_depart_prices[day]) < int(record_depart_prices[day]) ):
                record_broken_depart_prices[day] = todays_snapshot_depart_prices[day]
            else:
                record_broken_depart_prices[day] = record_depart_prices[day]

        if record_broken_depart_prices != record_depart_prices:
            record_prices.update_item(
                Key={'user_id': user_id},
                UpdateExpression="SET prices.depart_prices = :p",
                ExpressionAttributeValues={
                    ':p': record_broken_depart_prices
                },
                ReturnValues="UPDATED_NEW"
            )


    return {
        'statusCode': 200
    }
