import pymysql
import boto3
import os
import json
from datetime import datetime

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
    # Extract categoryId from the request body
    body = json.loads(event.get('body', '{}'))
    category_id = body.get('categoryId')

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

        # Retrieve questions that match the categoryId and have a value in the answer field
        cur = conn.cursor()
        select_query = """
            SELECT id, category, text, live, completion_date, answer_date, answer, correct
            FROM questions
            WHERE category = %s AND (answer IS NOT NULL AND answer != '')
        """
        cur.execute(select_query, (category_id,))
        query_results = cur.fetchall()

        # Format the result as a list of dictionaries
        questions_list = []
        for row in query_results:
            question_id = row[0]
            total_users_query = """
                SELECT COUNT(*)
                FROM answers
                WHERE question_id = %s
            """
            cur.execute(total_users_query, (question_id,))
            total_users = cur.fetchone()[0]

            answered_yes_query = """
                SELECT COUNT(*)
                FROM answers
                WHERE question_id = %s AND user_answer = 1
            """
            cur.execute(answered_yes_query, (question_id,))
            answered_yes_count = cur.fetchone()[0]

            answered_yes_percentage = (answered_yes_count / total_users) * 100 if total_users > 0 else 0
            print(answered_yes_percentage)

            question = {
                'id': question_id,
                'category': row[1],
                'text': row[2],
                'live': row[3].strftime('%Y-%m-%d %H:%M:%S'),  # Convert datetime to string
                'completion_date': row[4].strftime('%Y-%m-%d %H:%M:%S'),  # Convert datetime to string
                'answer_date': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] is not None else None,
                'answer': row[6] if row[6] is not None else None,
                'correct': row[7] if row[7] is not None else None,
                'answeredYes': answered_yes_percentage
            }
            questions_list.append(question)

        conn.close()

        # Return the result as JSON
        return json.dumps(questions_list)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve questions"
