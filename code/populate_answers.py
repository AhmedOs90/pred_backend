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
        collected_category_ids = set()

        for question in questions:
            question_id = question[0]
            category_id = question[1]
            live_date = question[3]
            expiration_date = question[4]
            populated = question[8] if len(question) > 8 else False

            # Check if the question is auto-populated                
            # Check if the question is live
            if current_time < live_date:
                print("not live yet")
                continue

            # Check if the question has expired
            expire_time = live_date + timedelta(hours=24)
            if current_time > expire_time:
                print("expired")
                continue

            if not populated:
                # Mark the question as auto-populated
                print("not populated")
                mark_question_as_populated(question_id)
                collected_category_ids.add(category_id)

                # Get all user IDs subscribed to the question's category
                user_ids = get_subscribed_user_ids(category_id)

                # Insert the answer into the answers table for each user
                for user_id in user_ids:
                    insert_answer(user_id, question_id, category_id)
            else:
                print("already populated")

        collected_category_ids_list = list(collected_category_ids)
        
        if collected_category_ids_list:
            send_notification(collected_category_ids_list)
        return "Questions processed successfully"
    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to process questions"


def mark_question_as_populated(question_id):
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
        update_query = "UPDATE questions SET populated = true WHERE id = %s"
        cur.execute(update_query, (question_id,))
        conn.commit()
        conn.close()

        print("Question marked as populated")

    except Exception as e:
        print("Database connection failed due to {}".format(e))


def insert_answer(user_id, question_id, category):
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
        insert_query = "INSERT INTO answers (user_id, question_id, category) VALUES (%s, %s, %s)"
        cur.execute(insert_query, (user_id, question_id, category))
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
        select_query = """
            SELECT u.id
            FROM users AS u
            JOIN user_cats AS uc ON u.id = uc.user_id
            WHERE uc.category_id = %s AND u.status = 1
        """
        cur.execute(select_query, (category_id,))
        user_ids = [row[0] for row in cur.fetchall()]
        print("Getting User IDs")
        print(user_ids)
        conn.close()
        return user_ids

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return []

def send_notification(category_ids):
    try:
        sns_client = boto3.client('sns', region_name='us-east-1')

        topic_arn = 'arn:aws:sns:us-east-1:896988986379:Predictation'

        message_attributes = {
            'categoryId': {
                'DataType': 'String.Array',
                'StringValue': f"{category_ids}"
            }
        }
        
        
        message = {
            "default": "Sample fallback message",
            "GCM": json.dumps({
                "notification": {
                    "body": "You have a new question",
                    "title": "New Questions Available",
                },
                "data": {
                    "type": "question"
                }
            })
        }

        dubed = json.dumps(message)
        print(dubed);
        print(message_attributes)
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageStructure='json',
            MessageAttributes=message_attributes
        )
        print(response)

        print('Message published:', response['MessageId'])

    except Exception as e:
        print('Error sending notification:', e)



def lambda_handler(event, context):
    return get_questions(event, context)
