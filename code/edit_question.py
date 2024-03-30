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
    body = json.loads(event.get('body', '{}'))

    # Extract question data from the event
    question_id = body.get('id')
    category = body.get('category')
    text = body.get('text')
    live = body.get('live')
    completion_date = body.get('completion_date')
    answer_date = body.get('answer_date')
    answer = body.get('answer')
    correct = body.get('correct')

    live = datetime.strptime(live, '%Y-%m-%d %H:%M:%S')
    completion_date = datetime.strptime(completion_date, '%Y-%m-%d %H:%M:%S')

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

        # Update the question with the specified question_id
        cur = conn.cursor()
        update_query = """
            UPDATE questions
            SET category = %s, text = %s, live = %s, completion_date = %s, answer_date = %s, answer = %s, correct = %s
            WHERE id = %s
        """
        update_data = (category, text, live, completion_date, answer_date, answer, correct, question_id)
        cur.execute(update_query, update_data)
        conn.commit()
        conn.close()

        return "Question updated successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to update question"
