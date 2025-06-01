import boto3
from boto3.dynamodb.types import TypeDeserializer

dynamodb = boto3.resource('dynamodb')
record_prices_table = dynamodb.Table('Record_Prices')

def ddb_deserialize(r, type_deserializer = TypeDeserializer()):
    return type_deserializer.deserialize({"M": r})

def update_record_table_if_record_broken(user_id, record_prices, todays_snapshot_prices, date_scraped, depart_or_return):
    broken_prices = {}

    for day in record_prices:
        if ( int(todays_snapshot_prices[day]) < int(record_prices[day]) ):
            broken_prices[day] = todays_snapshot_prices[day]
        else:
            broken_prices[day] = record_prices[day]

    if broken_prices != record_prices:
        record_prices_table.update_item(
            Key={'user_id': user_id},
            UpdateExpression=f"SET prices.{depart_or_return}_prices = :p, date_scraped = :ds",
            ExpressionAttributeValues={
                ':p': broken_prices,
                ':ds': date_scraped
            },
            ReturnValues="UPDATED_NEW"
        )

def lambda_handler(event, context):
    try:
        print(event)
        eventDeserialized = [ ddb_deserialize(r["dynamodb"]["NewImage"]) for r in event['Records'] ][0]
        new_user = eventDeserialized['new_user']

        # Only process if not new user. New users will automatically have the snapshot inserted into record_prices table
        if (new_user == 'N'):
            user_id = eventDeserialized['user_id']
            date_scraped = eventDeserialized['date_scraped']
            todays_snapshot_depart_prices = eventDeserialized['prices']['depart_prices']
            todays_snapshot_return_prices = eventDeserialized['prices']['return_prices']
            record_prices_to_date = record_prices_table.get_item(Key={'user_id': user_id})
            record_depart_prices = record_prices_to_date['Item']['prices']['depart_prices']
            record_return_prices = record_prices_to_date['Item']['prices']['return_prices']

            update_record_table_if_record_broken(user_id, record_depart_prices, todays_snapshot_depart_prices, date_scraped, 'depart')
            update_record_table_if_record_broken(user_id, record_return_prices, todays_snapshot_return_prices, date_scraped, 'return')

        return {
            'statusCode': 200
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500
        }
