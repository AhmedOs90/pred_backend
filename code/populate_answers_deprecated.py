import pymysql
import boto3
import os
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


def get_questions(event, context):
    session = boto3.Session()
    client = session.client('rds')
    token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

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
        select_query = "SELECT * FROM questions"
        cur.execute(select_query)
        questions = cur.fetchall()
        conn.close()

        current_time = datetime.now()

        for question in questions:
            question_id = question[0]
            category_id = question[1]
            live_date = question[3]
            expiration_date = question[4]

            # Check if the question is live
            if current_time < live_date:
                print("not live yet")
                continue

            # Check if the question has expired
            expire_time = live_date + timedelta(hours=24)
            if current_time > expire_time:
                print("expired")
                continue

            # Get all user IDs subscribed to the question's category
            user_ids = get_subscribed_user_ids(category_id)

            # Insert the answer into the answers table for each user
            for user_id in user_ids:
                if is_answer_exists(user_id, question_id):
                    print("Answer already exists")
                    continue
                insert_answer(user_id, question_id)


        return "Questions processed successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to process questions"

def is_answer_exists(user_id, question_id):
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
        select_query = "SELECT * FROM answers WHERE user_id = %s AND question_id = %s"
        cur.execute(select_query, (user_id, question_id))
        result = cur.fetchone()
        conn.close()

        if result:
            return True  # Answer already exists for the given user and question
        else:
            return False  # Answer does not exist

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return False

def insert_answer(user_id, question_id):
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
        insert_query = "INSERT INTO answers (user_id, question_id) VALUES (%s, %s)"
        cur.execute(insert_query, (user_id, question_id))
        print("Question inserted")
        conn.commit()
        conn.close()

        return "Answer inserted successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to insert answer"

def get_subscribed_user_ids(category_id):
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
        select_query = "SELECT user_id FROM user_cats WHERE category_id = %s"
        cur.execute(select_query, (category_id,))
        user_ids = [row[0] for row in cur.fetchall()]
        print("Getting User IDs")
        print(user_ids)
        conn.close()

        return user_ids

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return []

def lambda_handler(event, context):
    return get_questions(event, context)
