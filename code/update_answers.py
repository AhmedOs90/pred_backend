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


def lambda_handler(event, context):
    # Extract parameters from the event
    body = json.loads(event.get('body', '{}'))
    user_id = body.get('userId')
    question_id = body.get('questionId')
    user_answer = body.get('answer')

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

        # Check if the question is expired
        cur = conn.cursor()
        select_query = "SELECT live FROM questions WHERE id = %s"
        cur.execute(select_query, (question_id,))
        result = cur.fetchone()
        
        if not result:
            conn.close()
            return {
                'statusCode': 400,
                'body': "Question not found"
            }
        live_date = result[0]
        expiration_date = live_date + timedelta(hours=24)  # Calculate expiration date as live date plus 24 hours
        current_time = datetime.now()
        
        if current_time > expiration_date:
            conn.close()
            return {
                'statusCode': 400,
                'body': "Question has expired"
            }

        # Update the user_answer in the answers table
        update_query = """
            UPDATE answers
            SET user_answer = %s
            WHERE user_id = %s AND question_id = %s
        """
        cur.execute(update_query, (user_answer, user_id, question_id))
        conn.commit()
        conn.close()
        return "Answer updated successfully"

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to update answer"
