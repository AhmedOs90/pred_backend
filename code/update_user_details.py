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
            return False  # Username already exists
        else:
            return True  # Username does not exist

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return False  # Return False in case of any error


def update_user(user_id, username, email, registration_date, year_of_birth, country, language, admin, reg_partner, status):
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
            SET username = %s, email = %s, registration_date = %s, year_of_birth = %s, country = %s, language = %s, admin = %s, reg_partner = %s, status = %s 
            WHERE id = %s
        """

        # Convert registration_date to datetime object
        registration_date = datetime.strptime(registration_date, '%Y-%m-%d %H:%M:%S')

        update_data = (username, email, registration_date, year_of_birth, country, language, admin, reg_partner, status, user_id)
        cur.execute(update_query, update_data)
        conn.commit()
        conn.close()

        # Return updated user details
        updated_user = {
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

        return updated_user

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return None  # Return None in case of any error

def get_user(user_id):
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
        select_query = "SELECT * FROM users WHERE id = %s"
        cur.execute(select_query, (user_id,))
        result = cur.fetchone()
        conn.close()

        if result:
            user = {
                'id': result[0],
                'username': result[1],
                'email': result[2],
                'registration_date': result[3].strftime('%Y-%m-%d %H:%M:%S'),
                'year_of_birth': result[4],
                'country': result[5],
                'language': result[6],
                'admin': result[7],
                'reg_partner': result[8],
                'status': result[9]
            }
            return user
        else:
            return None

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return None  # Return None in case of any error



def lambda_handler(event, context):
    # Get user details from the event
    body = json.loads(event.get('body', '{}'))

    user_id = body.get('userId')
    username = body.get('username')
    email = body.get('email')
    registration_date = body.get('registration_date')
    year_of_birth = body.get('year_of_birth')
    country = body.get('country')
    language = body.get('language')
    admin = body.get('admin')
    reg_partner = body.get('reg_partner')
    status = body.get('status')

    # Retrieve the current user details from the database
    current_user = get_user(user_id)

    if not current_user:
        return {
            'statusCode': 404,
            'body': "User does not exist"
        }

    # Check if the provided username is different from the current username
    if username != current_user['username']:
        # Check if the new username is unique
        if not check_username(username):
            return {
                'statusCode': 400,
                'body': "Username already exists"
            }

    # Continue with the update process
    updated_user = update_user(user_id, username, email, registration_date, year_of_birth, country, language, admin,
                               reg_partner, status)

    if updated_user:
        return updated_user  # Return the updated user details
    else:
        return "Failed to update user"  # Return an error message if the update failed

