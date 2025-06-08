import boto3
from boto3.dynamodb.types import TypeDeserializer

dynamodb = boto3.resource('dynamodb')
record_prices_table = dynamodb.Table('Record_Prices')

def ddb_deserialize(r, type_deserializer = TypeDeserializer()):
    return type_deserializer.deserialize({"M": r})

def update_record_table_if_record_broken(user_id, record_depart_prices, record_return_prices, todays_snapshot_depart_prices, todays_snapshot_return_prices, date_scraped):
    broken_depart_prices = {}
    broken_return_prices = {}

    for day in record_depart_prices:
        if int(todays_snapshot_depart_prices[day]) < int(record_depart_prices[day]):
            broken_depart_prices[day] = todays_snapshot_depart_prices[day]
        else:
            broken_depart_prices[day] = record_depart_prices[day]

    for day in record_return_prices:
        if int(todays_snapshot_return_prices[day]) < int(record_return_prices[day]):
            broken_return_prices[day] = todays_snapshot_return_prices[day]
        else:
            broken_return_prices[day] = record_return_prices[day]

    # Only update if either depart or return prices changed
    if (broken_depart_prices != record_depart_prices or broken_return_prices != record_return_prices):
        record_prices_table.update_item(
            Key={'user_id': user_id},
            UpdateExpression="""
                SET
                    prices.depart_prices = :depart_prices,
                    prices.return_prices = :return_prices,
                    date_scraped = :ds
            """,
            ExpressionAttributeValues={
                ':depart_prices': broken_depart_prices,
                ':return_prices': broken_return_prices,
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

            update_record_table_if_record_broken(user_id, record_depart_prices, record_return_prices, todays_snapshot_depart_prices, todays_snapshot_return_prices, date_scraped)

        return {
            'statusCode': 200
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500
        }
