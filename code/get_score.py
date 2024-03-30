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
    # Extract userId and category list from the event
    body = json.loads(event.get('body', '{}'))
    user_id = body.get('userId')
    category_list = body.get('categoryList', [])  # Default to empty list if not provided

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

        total_all_answers = 0
        total_result_1 = 0
        total_difficulty = 0  # Initialize the total difficulty sum
        rank = 0  # Initialize the rank

        cur = conn.cursor()

        for category_id in category_list:
            # Retrieve the counts of answers and sum of difficulty for the specified user and category
            select_query_all = """
                SELECT COUNT(*) 
                FROM answers
                WHERE user_id = %s AND category = %s AND result IS NOT NULL
            """
            select_query_result_1 = """
                SELECT COUNT(*) 
                FROM answers
                WHERE user_id = %s AND result = 1 AND category = %s
            """
            select_query_difficulty = """
                SELECT SUM(difficulty) 
                FROM answers
                WHERE user_id = %s AND category = %s AND result = 1 AND difficulty IS NOT NULL
            """
            cur.execute(select_query_all, (user_id, category_id))
            count_all = cur.fetchone()[0]
            cur.execute(select_query_result_1, (user_id, category_id))
            count_result_1 = cur.fetchone()[0]
            cur.execute(select_query_difficulty, (user_id, category_id))
            difficulty_sum = cur.fetchone()[0]

            total_all_answers += count_all
            total_result_1 += count_result_1
            if difficulty_sum is not None:  # Check if difficulty_sum is not None before adding
                total_difficulty += difficulty_sum

            # If there is only one category in the list, retrieve the rank
            if len(category_list) == 1:
                select_rank_query = """
                    SELECT percentile_rank
                    FROM user_cats
                    WHERE user_id = %s AND category_id = %s
                """
                cur.execute(select_rank_query, (user_id, category_id))
                rank_result = cur.fetchone()
                if rank_result:
                    rank = rank_result[0]

        result = {
            "totalAllAnswers": total_all_answers,
            "totalAnswersWithResult1": total_result_1,
            "totalDifficultySum": total_difficulty,
            "rank": rank  # Include the rank in the result
        }

        conn.close()

        return result

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve answer counts and difficulty sum"
