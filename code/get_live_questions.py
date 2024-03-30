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
    # Extract user ID from the event
    body = json.loads(event.get('body', '{}'))

    # Extract user ID from the request body
    user_id = body.get('userId')
    print(user_id)
    print(event.get("body"))
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

        # Retrieve the list of unanswered question IDs for the user
        cur = conn.cursor()
        select_query = """
            SELECT question_id
            FROM answers
            WHERE user_id = %s AND user_answer IS NULL
        """
        cur.execute(select_query, (user_id,))
        question_ids = [row[0] for row in cur.fetchall()]

        # Retrieve the unanswered questions based on the question IDs
        questions_list = []
        if question_ids:
            select_query = """
                SELECT id, category, text, live, completion_date, answer_date, answer, correct
                FROM questions
                WHERE id IN ({})
                ORDER BY live ASC
            """.format(','.join(['%s'] * len(question_ids)))

            cur.execute(select_query, question_ids)
            query_results = cur.fetchall()

            # Format the result as a list of dictionaries
            for row in query_results:
                live_date = row[3]
                hours = 24  # Specify the duration in hours
                expiration_date = live_date + timedelta(hours=hours)
                current_time = datetime.now()
                print(live_date)
                print(expiration_date)

                if current_time < expiration_date:
                    question = {
                        'id': row[0],
                        'category': row[1],
                        'text': row[2],
                        'live': row[3].strftime('%Y-%m-%d %H:%M:%S'),  # Convert datetime to string
                        'completion_date': row[4].strftime('%Y-%m-%d %H:%M:%S'),  # Convert datetime to string
                        'answer_date': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] is not None else None,
                        'answer': row[6] if row[6] is not None else None,
                        'correct': row[7] if row[7] is not None else None
                    }
                    questions_list.append(question)

        conn.close()

        # Return the result as JSON
        return json.dumps(questions_list)

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve questions"
