import pymysql
import boto3
import os
import json
from datetime import datetime, timedelta


ENDPOINT = "predictative-instance-1.cb9thpviu0kz.us-east-1.rds.amazonaws.com"
PORT = 3306
USER = "predictuser"
REGION = "us-east-1"
DBNAME = "predictdb"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
session = boto3.Session()
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)




def remove_user_data(user_id):
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
        update_query = """
            UPDATE users 
            SET email = NULL, username = NULL, country = NULL, year_of_birth = NULL, status = 3
            WHERE id = %s
        """

        cur.execute(update_query, (user_id,))
        conn.commit()
        conn.close()

        return "User data removed successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to remove user data"


def lambda_handler(event, context):
    # Get user details from the event
    body = json.loads(event.get('body', '{}'))

    user_id = body.get('userId')

    # Remove the user data from the row in the database
    result = remove_user_data(user_id)

    return {
        'statusCode': 200,
        'body': result
    }
