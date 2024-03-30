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
    # Extract userId from the event
    body = json.loads(event.get('body', '{}'))
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

        # Retrieve the list of answers for the specified user
        cur = conn.cursor()
        select_query = """
            SELECT q.text AS question_text, q.answer AS correct_answer, a.user_answer, q.correct, q.id AS question_id, q.completion_date
            FROM questions AS q
            LEFT JOIN answers AS a ON q.id = a.question_id AND a.user_id = %s
            WHERE a.user_answer IS NOT NULL 
            AND q.answer IS NOT NULL AND q.answer != ''
            ORDER BY q.answer_date DESC
            LIMIT 10
        """
        cur.execute(select_query, (user_id,))
        query_results = cur.fetchall()

        # Format the result as a list of dictionaries
        answers_list = []
        for row in query_results:
            answer = {
                'question_text': row[0],
                'correct_answer': row[1],
                'user_answer': row[2] if row[2] is not None else None,
                'correct_percent': row[3],
                'question_id': row[4],
                'completion_date': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] is not None else None,
            }
            answers_list.append(answer)

        conn.close()

        return json.dumps(answers_list)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve answers"
