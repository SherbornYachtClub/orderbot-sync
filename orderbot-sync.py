#!/usr/bin/env python3

import os
import sys
import logging
import requests
import json
import psycopg2
from psycopg2 import sql
from datetime import date, datetime, timedelta
from dateutil import parser as parsedate

## function to get a nested list of all orders from squarespace
## returns: dictionary of orders from json result
def squarespace_items(api_endpoint, json_return, parameters):
    item_list = []
    commerce_creds = os.environ.get('SQUARESPACE_API_KEY')

    # define JSON headers, API key pulled from environment variable SQUARESPACE_API_KEY
    headers = {
        "Authorization": "Bearer " + commerce_creds,
        "User-Agent": "MembershipBot"
    }

    response = requests.get(api_endpoint, headers=headers, params=parameters)
    if response.raise_for_status():
        pass
    else:
        json_data = response.json()

    if response.status_code == requests.codes.ok:
        for item in json_data[json_return]:
            item_list.append(item)

        if json_data['pagination']['hasNextPage']:
            return (item_list + squarespace_items(json_data['pagination']['nextPageUrl'], json_return, None))
    else:
        logging.error("Return status from response was requests.get was NOT OK: %s" % response.status_code)
        return False

    return item_list

def squarespace_orders_json():
    orders_api_endpoint = "https://api.squarespace.com/1.0/commerce/orders"
    current_year = datetime.now().year

    year_beginning = date(current_year, 1, 1).isoformat()+ 'T00:00:00.0Z'
    year_end = date(current_year, 12, 30).isoformat()+ 'T00:00:00.0Z'

    # define JSON parameters for the date range to get orders
    request_parameters = {
        "modifiedAfter": year_beginning,
        "modifiedBefore": year_end,
    }

    # Get all transactions for specified year
    # the json result will be of documents type
    try:
        json_var = 'result'
        orders = squarespace_items(orders_api_endpoint, json_var, request_parameters)
        if not orders:
            logging.warning("No orders since the beginning of the year")

    except requests.exceptions.HTTPError as error:
        logging.error("Failed to get new orders: %s" % error)

    return orders

def insert_items(items):
    # Get database credentials from environment variables
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASS")
    db_server = os.environ.get("DB_SERVER")

    # Construct the connection string
    db_conn_string = f'postgresql://{db_user}:{db_password}@{db_server}.us-west-2.retooldb.com/retool?sslmode=require'

    # Connect to the database
    try:
        conn = psycopg2.connect(db_conn_string)
        cursor = conn.cursor()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Database connection error: {str(e)}")
        }

    for payload in items:
            for line_item in payload['lineItems']:

                # Handle missing variantOptions
                variant_options = json.dumps(line_item['variantOptions']) if 'variantOptions' in line_item else None

                try:
                    query = sql.SQL("""
                        INSERT INTO syc_orders (
                            id, order_number, created_on, modified_on, channel, testmode, customer_email,
                            billing_first_name, billing_last_name, billing_address1, billing_address2,
                            billing_city, billing_state, billing_country_code, billing_postal_code, billing_phone,
                            fulfillment_status, line_item_id, variant_id, variant_options, sku, product_id, product_name,
                            quantity, unit_price_paid, image_url, line_item_type, customizations, subtotal, shipping_total,
                            discount_total, tax_total, refunded_total, grand_total, channel_name, external_order_reference,
                            fulfilled_on, price_tax_interpretation
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """)
                    values = (
                        line_item['id'], payload['orderNumber'], payload['createdOn'], payload['modifiedOn'], payload['channel'], payload['testmode'],
                        payload['customerEmail'], payload['billingAddress']['firstName'], payload['billingAddress']['lastName'],
                        payload['billingAddress']['address1'], payload['billingAddress']['address2'], payload['billingAddress']['city'],
                        payload['billingAddress']['state'], payload['billingAddress']['countryCode'], payload['billingAddress']['postalCode'],
                        payload['billingAddress']['phone'], payload['fulfillmentStatus'], line_item['id'], line_item['variantId'],
                        variant_options, line_item['sku'], line_item['productId'], line_item['productName'], line_item['quantity'],
                        line_item['unitPricePaid']['value'], line_item['imageUrl'], line_item['lineItemType'],
                        json.dumps(line_item['customizations']), payload['subtotal']['value'], payload['shippingTotal']['value'],
                        payload['discountTotal']['value'], payload['taxTotal']['value'], payload['refundedTotal']['value'],
                        payload['grandTotal']['value'], payload['channelName'], payload['externalOrderReference'], payload['fulfilledOn'],
                        payload['priceTaxInterpretation']
                    )

                    #print(f"Executing query with values: {values}")  # Debug log to show the values being inserted
                    cursor.execute(query, values)
                except psycopg2.errors.UniqueViolation as e:
                    print("Caught duplicate lineitem, skipping: %s" % e)
                except psycopg2.errors.InFailedSqlTransaction as e:
                    print("Some error in the transaction: %s" % e)

            conn.commit()

    cursor.close()
    conn.close()

    print("Successful load out")

    return 0

def main():
    # Setup logging
    if os.environ.get('SQUARESPACE_API_KEY') is None:
        logging.critical("Failed to pass SQUARESPACE_API_KEY. Exiting")
        return 1

    LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
    logging.basicConfig(level=LOGLEVEL)

    orders = squarespace_orders_json()
    insert_items(orders)

    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully!')
    }

def handler(event, context):
    return main()

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.critical("Caught a control-C. Bailing out")
