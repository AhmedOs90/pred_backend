import pymysql
import boto3
import os
import json

ENDPOINT = "predictative-instance-1.cb9thpviu0kz.us-east-1.rds.amazonaws.com"
PORT = 3306
USER = "predictuser"
REGION = "us-east-1"
DBNAME = "predictdb"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

session = boto3.Session()
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

def lambda_handler(event, context):
    # Extract user_id and category_ids from the event
    body = json.loads(event.get('body', '{}'))

    user_id = body.get('user_id')
    category_ids = body.get('category_ids', [])

    # Connect to the database
    try:
        conn = pymysql.connect(
            host=ENDPOINT,
            user=USER,
            passwd=token,
            password="RYYxtwbl2Ig4Y1jV",
            port=PORT,
            database=DBNAME,
            ssl={'ca': 'global-bundle.pem'}
        )

        cur = conn.cursor()

        # Delete existing rows with the given user_id
        delete_query = "DELETE FROM user_cats WHERE user_id = %s"
        cur.execute(delete_query, (user_id,))
        conn.commit()

        # Insert rows for the given user_id and each category_id in the list
        insert_query = "INSERT INTO user_cats (user_id, category_id) VALUES (%s, %s)"
        for category_id in category_ids:
            insert_data = (user_id, category_id)
            cur.execute(insert_query, insert_data)

        conn.commit()
        conn.close()

        return "Rows inserted successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to insert rows"
