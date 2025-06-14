from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import boto3
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

dynamodb = boto3.resource('dynamodb')
user_table = dynamodb.Table('Users')
history_table = dynamodb.Table('History')
record_prices = dynamodb.Table('Record_Prices')

def extract_prices_from_calendar(calendar_div):
    prices = {}

    for button in calendar_div.find_all('button', class_='month-view-calendar__cell'):
    # Skip previous month dates. Eg. I looked up August, but the UI was also giving July 30 31 dates
        if 'month-view-calendar__cell--blocked' in button.get('class', []):
            continue

        date = button.find('p', class_='BpkText_bpk-text__ZjI3M BpkText_bpk-text--label-1__MWI4N date').get_text(strip=True)
        price_in_green_text = button.find('p', class_='BpkText_bpk-text__ZjI3M BpkText_bpk-text--label-3__MjRmM price')
        price_in_black_text = button.find('p', class_='BpkText_bpk-text__ZjI3M BpkText_bpk-text--caption__NzU1O price')

        if price_in_green_text:
            price = price_in_green_text.get_text(strip=True).lstrip('$')
        elif price_in_black_text and price_in_black_text.find('svg'):
            # Dates which have no price yet set to 99999 initially
            price = '99999'
        else:
            price = price_in_black_text.get_text(strip=True).lstrip('$')

        prices[date] = price

    return prices

def user_exists(email):
    response = user_table.get_item(
        Key={
            'email': email
        }
    )
    return 'Item' in response

def insert_item_into_history_table(email, url, depart_prices, return_prices, new_user):
    user_id = user_table.get_item(Key={'email': email})['Item']['user_id']
    url = user_table.get_item(Key={'email': email})['Item']['url']

    history_table.put_item(
        Item={
            'id': str(uuid.uuid4()),
            'user_id': user_id,
            url: url,
            'date_scraped': datetime.today().strftime('%d-%m-%Y'),
            'prices': {
                'depart_prices': depart_prices,
                'return_prices': return_prices
                },
            'new_user': new_user
            }
    )

def insert_item_into_record_table(email, url, depart_prices, return_prices):
    user_id = user_table.get_item(Key={'email': email})['Item']['user_id']

    record_prices.put_item(
        Item={
            'user_id': user_id,
            'email': email,
            'url': url,
            'date_scraped': datetime.today().strftime('%d-%m-%Y'),
            'prices': {
                'depart_prices': depart_prices,
                'return_prices': return_prices
                }
            }
    )

def main():
    print(datetime.now())
    load_dotenv()
    skyscanner_url = os.getenv('url')
    email = os.getenv('email')
    firefox_options = Options()
    firefox_options.add_argument("--enable-javascript")
    firefox_options.add_argument("--headless")
    firefox_options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Firefox(options=firefox_options)
    driver.get(skyscanner_url)
    html_source = driver.page_source
    soup = BeautifulSoup(html_source, 'html.parser')

    depart_month_div = soup.find('div', class_='outbound-calendar')
    return_month_div = soup.find('div', class_='inbound-calendar')

    depart_prices = extract_prices_from_calendar(depart_month_div)
    return_prices = extract_prices_from_calendar(return_month_div)
    print(depart_prices)
    print(return_prices)

    driver.quit()

    if (user_exists(email)):
        new_user = 'N'
        insert_item_into_history_table(email, skyscanner_url, depart_prices, return_prices, new_user)
        print("successfully inserted existing user")
    else:
        new_user = 'Y'
        #if new user, insert into user table + insert record into history table + insert into record_prices as it is the first snapshot
        user_table.put_item(
            Item={
                'email': email,
                'url': skyscanner_url,
                'user_id': str(uuid.uuid4())
                }
        )

        insert_item_into_history_table(email, skyscanner_url, depart_prices, return_prices, new_user)
        insert_item_into_record_table(email, skyscanner_url, depart_prices, return_prices)
        print("successfully inserted new user")

if __name__ == "__main__":
    main()