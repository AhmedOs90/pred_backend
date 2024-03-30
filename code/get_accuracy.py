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

        if category_id == 0:
            # Retrieve the total number of questions with non-null answers for the user in all categories
            cur = conn.cursor()
            select_total_query = """
                SELECT COUNT(*)
                FROM questions AS q
                JOIN answers AS a ON q.id = a.question_id
                WHERE a.user_id = %s AND q.answer IS NOT NULL AND q.answer != ''
            """
            cur.execute(select_total_query, (user_id,))
            total_questions = cur.fetchone()[0]

            # Retrieve the number of correct answers for the user in all categories
            select_correct_query = """
                SELECT COUNT(*)
                FROM questions AS q
                JOIN answers AS a ON q.id = a.question_id
                WHERE a.user_id = %s AND q.answer = a.user_answer
            """
            cur.execute(select_correct_query, (user_id,))
            correct_answers = cur.fetchone()[0]
        else:
            # Retrieve the total number of questions with non-null answers for the user in the specified category
            cur = conn.cursor()
            select_total_query = """
                SELECT COUNT(*)
                FROM questions AS q
                JOIN answers AS a ON q.id = a.question_id
                WHERE q.category = %s AND a.user_id = %s AND q.answer IS NOT NULL AND q.answer != ''
            """
            cur.execute(select_total_query, (category_id, user_id))
            total_questions = cur.fetchone()[0]

            # Retrieve the number of correct answers for the user in the specified category
            select_correct_query = """
                SELECT COUNT(*)
                FROM questions AS q
                JOIN answers AS a ON q.id = a.question_id
                WHERE q.category = %s AND a.user_id = %s AND q.answer = a.user_answer
            """
            cur.execute(select_correct_query, (category_id, user_id))
            correct_answers = cur.fetchone()[0]

        conn.close()

        # Calculate the percentage of correct answers
        percentage = (correct_answers / total_questions) * 100 if total_questions > 0 else 0

        return percentage

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to calculate percentage of correct answers"
