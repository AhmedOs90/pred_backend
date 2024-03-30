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

        # Insert the row into the questions table
        cur = conn.cursor()
        insert_query = "INSERT INTO questions (id, category, text, live, completion_date, answer_date, answer, correct) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        insert_data = (question_id, category, text, live, completion_date, answer_date, answer, correct)
        cur.execute(insert_query, insert_data)
        conn.commit()
        conn.close()

        return "Row inserted successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to insert row"


# Test the Lambda function locally
# event = {
#     'id': 1,
#     'category': 0,
#     'text': "Sample question text",
#     'live': "2023-06-25 10:00:00",
#     'completion_date': "2023-06-25 12:00:00",
#     'answer_date': "2023-06-25 11:30:00",
#     'answer': "1",
#     'correct': 76
# }
# result = lambda_handler(event, {})
# print(result)
