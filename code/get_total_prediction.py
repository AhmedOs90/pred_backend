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
    # Extract categoryId and userId from the event
    body = json.loads(event.get('body', '{}'))
    category_id = body.get('categoryId')
    user_id = body.get('userId')

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

        # Retrieve the count of answers with result = 1 for the specified user and category
        cur = conn.cursor()
        select_query = """
            SELECT COUNT(*) 
            FROM answers
            WHERE user_id = %s AND result IS NOT NULL
        """
        if category_id != 0:
            select_query += "AND category = %s"
            cur.execute(select_query, (user_id, category_id))
        else:
            cur.execute(select_query, (user_id,))

        count = cur.fetchone()[0]

        conn.close()

        return count

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve count of answers"
