import pymysql
import sys
import json
import boto3
import os

ENDPOINT = "predictative-instance-1.cb9thpviu0kz.us-east-1.rds.amazonaws.com"
PORT = 3306
USER = "predictuser"
REGION = "us-east-1"
DBNAME = "predictdb"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

session = boto3.Session()
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

def check_user(email):
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
        select_query = "SELECT * FROM users WHERE email = %s"
        cur.execute(select_query, (email,))
        result = cur.fetchone()
        conn.close()

        if result:
            # Convert datetime to string
            result = {
                "id": result[0],
                "username": result[1],
                "email": result[2],
                "registration_date": result[3].strftime('%Y-%m-%d %H:%M:%S'),
                "year_of_birth": result[4],
                "country": result[5],
                "language": result[6],
                "admin": result[7],
                "reg_partner": result[8],
                "status": result[9]
            }
            return result  # User already exists
        else:
            return None  # User does not exist

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return None  # Return None in case of any error



def lambda_handler(event, context):
    # gets the credentials from .aws/credentials
    
    try:
        # Get user details from the event
        body = json.loads(event.get('body', '{}'))

        # Extract user ID from the request body
        email = body.get('email')
        # Check if the user already exists
        user_details = check_user(email)

        if user_details:
            return user_details  # User already exists, return user details
        else:
            return False

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Database connection failed"  # Return an error message if database connection failed
