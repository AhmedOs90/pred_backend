import pymysql
import sys
import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError

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

def check_username(username):
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
        select_query = "SELECT * FROM users WHERE username = %s"
        cur.execute(select_query, (username,))
        result = cur.fetchone()
        conn.close()

        if result:
            return True  # Username already exists
        else:
            return False  # Username does not exist

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return True  # Return True (error) in case of any error

def insert_user(username, email, registration_date, year_of_birth, country, language, admin, reg_partner, status):
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
        insert_query = "INSERT INTO users (username, email, registration_date, year_of_birth, country, language, admin, reg_partner, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

        registration_date = datetime.strptime(registration_date, '%Y-%m-%d %H:%M:%S')

        insert_data = (username, email, registration_date, year_of_birth, country, language, admin, reg_partner, status)
        cur.execute(insert_query, insert_data)
        conn.commit()
        conn.close()
        user_id = cur.lastrowid

        inserted_user = {
            'id': user_id,
            'username': username,
            'email': email,
            'registration_date': registration_date.strftime('%Y-%m-%d %H:%M:%S'),
            'year_of_birth': year_of_birth,
            'country': country,
            'language': language,
            'admin': admin,
            'reg_partner': reg_partner,
            'status': status
        }

        return inserted_user
    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return None  # Return None in case of any error

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', {}))

        username = body.get('username')
        email = body.get('email')
        registration_date = body.get('registration_date')
        year_of_birth = body.get('year_of_birth')
        country = body.get('country')
        language = body.get('language')
        admin = body.get('admin')
        reg_partner = body.get('reg_partner')
        status = body.get('status')

        user_details = check_user(email)

        if user_details:
            return {
                'statusCode': 200,
                'body': json.dumps(user_details)
            }
        elif check_username(username):
            return {
                'statusCode': 409,
                'body': json.dumps({"message": "Error: Username already exists"})
            }
        else:
            inserted_user = insert_user(username, email, registration_date, year_of_birth, country, language, admin,
                                        reg_partner, status)

            if inserted_user:
                return {
                    'statusCode': 200,
                    'body': json.dumps(inserted_user)
                }
            else:
                return {
                    'statusCode': 500,
                    'body': json.dumps({"message": "Failed to insert user"})
                }

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Database connection failed"})
        }
