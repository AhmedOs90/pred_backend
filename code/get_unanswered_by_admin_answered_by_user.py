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
    # Extract user ID from the event
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

        # Retrieve the list of question IDs and user answers for the user
        cur = conn.cursor()
        select_query = """
            SELECT question_id, user_answer
            FROM answers
            WHERE user_id = %s AND result IS NULL AND user_answer IS NOT NULL
        """
        cur.execute(select_query, (user_id,))
        query_results = cur.fetchall()

        # Format the result as a list of dictionaries
        questions_list = [{'questionId': row[0], 'userAnswer': row[1]} for row in query_results]

        conn.close()

        # Return the result as JSON
        return json.dumps(questions_list)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve questions"
