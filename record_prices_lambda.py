import boto3
from boto3.dynamodb.types import TypeDeserializer

client = boto3.client('ses', region_name='ap-southeast-2')
type_deserializer = TypeDeserializer()

def deserialize(item):
    return {k: type_deserializer.deserialize(v) for k, v in item.items()}

def get_changed_days(old_prices, new_prices):
    changed = []
    for day in new_prices:
        old_price = int(old_prices[day])
        new_price = int(new_prices[day])
        if new_price < old_price:
            changed.append(day)
    return changed

def send_email(email, url, date, new_depart, new_return, old_depart=None, old_return=None):
    body_lines = []
    depart_days_changed = []
    return_days_changed = []

    if old_depart is None:
        body_lines.append(f"This is a snapshot of prices as of {date}:\n")
        body_lines.append("Depart Prices:")
        body_lines.extend(f"   Day {day}: ${price}" for day, price in new_depart.items())
        body_lines.append("")
        body_lines.append("Return Prices:")
        body_lines.extend(f"   Day {day}: ${price}" for day, price in new_return.items())
    else:
        depart_days_changed = get_changed_days(old_depart, new_depart)
        return_days_changed = get_changed_days(old_return, new_return)

        body_lines.append(f"Price update as of {date}:\n")

        if depart_days_changed:
            body_lines.append("Depart prices reached record low on:")
            for day in depart_days_changed:
                body_lines.append(
                    f"   Day {day}: ${old_depart[day]} → ${new_depart[day]}"
                )
        else:
            body_lines.append("No change in depart prices.")

        body_lines.append("")

        if return_days_changed:
            body_lines.append("Return prices reached record low on:")
            for day in return_days_changed:
                body_lines.append(
                    f"   Day {day}: ${old_return[day]} → ${new_return[day]}"
                )
        else:
            body_lines.append("No change in return prices.")

    body_lines.append(f"\nGo check it out at {url}")
    body_text = "\n".join(body_lines)

    if depart_days_changed or return_days_changed or old_depart is None:
        client.send_email(
            Destination={'ToAddresses': [email]},
            Message={
                'Body': {'Text': {'Charset': 'UTF-8', 'Data': body_text}},
                'Subject': {'Charset': 'UTF-8', 'Data': 'Prices as of ' + date},
            },
            Source=''
            )

def lambda_handler(event, context):
    try:
        print(event)
        newImage = event['Records'][0]['dynamodb']['NewImage']
        deserializedNewImage = deserialize(newImage)
        email = deserializedNewImage['email']
        date = deserializedNewImage['date_scraped']
        url = deserializedNewImage['url']

        # Dates in event object are not sorted. Deserialize + sort dates
        new_depart_prices = dict(sorted(deserializedNewImage['prices']['depart_prices'].items(), key=lambda x: int(x[0])))
        new_return_prices = dict(sorted(deserializedNewImage['prices']['return_prices'].items(), key=lambda x: int(x[0])))

        if event['Records'][0]['eventName'] == 'MODIFY':
            oldImage = event['Records'][0]['dynamodb']['OldImage']
            deserializedOldImage = deserialize(oldImage)
            old_depart_prices = dict(sorted(deserializedOldImage['prices']['depart_prices'].items(), key=lambda x: int(x[0])))
            old_return_prices = dict(sorted(deserializedOldImage['prices']['return_prices'].items(), key=lambda x: int(x[0])))

            send_email(email, url, date, new_depart_prices, new_return_prices, old_depart_prices, old_return_prices)
            return {'statusCode': 200}
        elif event['Records'][0]['eventName'] == 'INSERT':
            send_email(email, url, date, new_depart_prices, new_return_prices)
            return {'statusCode': 200}

        return {'statusCode': 200}
    except Exception as e:
        print(e)
        return {'statusCode': 500}