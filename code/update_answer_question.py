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
    # Extract questionId and answer from the request body
    body = json.loads(event.get('body', '{}'))
    question_id = body.get('questionId')
    answer = body.get('answer')
    category = body.get('category')


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

        # Check if the question is live for at least 24 hours
        cur = conn.cursor()
        select_live_query = """
            SELECT live
            FROM questions
            WHERE id = %s
        """
        cur.execute(select_live_query, (question_id,))
        live_date = cur.fetchone()[0]

        current_datetime = datetime.now()
        expire_time = live_date + timedelta(hours=24)

        if current_datetime < expire_time:
            return {
                'statusCode': 400,
                'body': "Question not Expired Yet"
            }

        # Update the question with the specified questionId
        update_query = """
            UPDATE questions
            SET answer = %s, answer_date = %s
            WHERE id = %s
        """
        cur.execute(update_query, (answer, current_datetime, question_id))
        conn.commit()

        # Update the corresponding answer in the answers table
        update_answer_query = """
            UPDATE answers
            SET result = CASE WHEN user_answer = %s THEN 1 ELSE 0 END
            WHERE question_id = %s
        """
        cur.execute(update_answer_query, (answer, question_id))
        conn.commit()

        # Calculate the percentage of correct answers for the question
        calculate_percentage_query = """
            SELECT AVG(result) * 100
            FROM answers
            WHERE question_id = %s
        """
        cur.execute(calculate_percentage_query, (question_id,))
        percentage = cur.fetchone()[0]

        difficulty = 100 - percentage
        # Update the correct field in the questions table with the percentage
        update_correct_query = """
            UPDATE questions
            SET correct = %s
            WHERE id = %s
        """
        update_difficulty_questions_query = """
            UPDATE questions
            SET difficulty = %s
            WHERE id = %s
        """
        update_difficulty_answers_query = """
            UPDATE answers
            SET difficulty = %s
            WHERE question_id = %s
        """
        cur.execute(update_correct_query, (percentage, question_id))
        cur.execute(update_difficulty_questions_query, (difficulty, question_id))
        cur.execute(update_difficulty_answers_query, (difficulty, question_id))

        send_notification(question_id, answer)
        conn.commit()

        conn.close()

        # Invoke the second Lambda function with the 'category' in the event body
        print("about to invoke calculation")
        print(category)
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        response = lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-east-1:896988986379:function:calculate_rank",
            InvocationType='Event',  # Use 'Event' for asynchronous invocation
            Payload=json.dumps({'body': json.dumps({'category': category})})
        )
        print(response)
        return {
                'statusCode': 200,
                'body': "Updated Successfully"
            }

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to update question, answer, and correct percentage"


def send_notification(questionId, answer):
    try:
        sns_client = boto3.client('sns', region_name='us-east-1')

        topic_arn = 'arn:aws:sns:us-east-1:896988986379:predictation-answers'

        # Send notification for the user's answer
        message_attributes = {
            'question': {
                'DataType': 'String.Array',
                'StringValue': f'["{questionId}_{answer}"]'
            }
        }
        message = {
            "default": "Sample fallback message",
            "GCM": json.dumps({
                "notification": {
                    "body": "Well done, you made a correct Predictation.",
                    "title": "New Predictation Answer",
                },
                "data": {
                    "type": "answer"
                }
            })
        }

        print(message_attributes)

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageStructure='json',
            MessageAttributes=message_attributes
        )

        # Send notification for the opposite of the user's answer
        opposite_answer = "1" if answer == "0" else "0"
        opposite_message_attributes = {
            'question': {
                'DataType': 'String.Array',
                'StringValue': f'["{questionId}_{opposite_answer}"]'
            }
        }
    
        opposite_message = {
            "default": "Sample fallback message",
            "GCM": json.dumps({
                "notification": {
                    "body": "Unfortunately, your Predictation did not come true.",
                    "title": "New Predictation Answer",   
                },
                "data": {
                    "type": "answer"
                }
            })
        }


        opposite_response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(opposite_message),
            MessageStructure='json',
            MessageAttributes=opposite_message_attributes
        )

        print('Messages published:', response['MessageId'], opposite_response['MessageId'])

    except Exception as e:
        print('Error sending notification:', e)


