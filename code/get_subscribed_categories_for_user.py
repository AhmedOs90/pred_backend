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
    # Extract user ID from the event
    body = json.loads(event.get('body', '{}'))
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

        # Retrieve categories for the user
        cur = conn.cursor()
        select_query = """
            SELECT c.id, c.name
            FROM categories AS c
            JOIN user_cats AS uc ON c.id = uc.category_id
            WHERE uc.user_id = %s
        """
        cur.execute(select_query, (user_id,))
        query_results = cur.fetchall()

        # Format the result as a list of dictionaries
        categories_list = []
        for row in query_results:
            category = {
                'id': row[0],
                'name': row[1]
            }
            categories_list.append(category)

        conn.close()

        # Return the result as JSON
        return json.dumps(categories_list)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve categories"
