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
    try:
        # Extract category from the event body
        body = json.loads(event.get('body', '{}'))
        category = body.get('category')
        
        print(category)
        # Connect to the database
        conn = pymysql.connect(
            host=ENDPOINT,
            user=USER,
            passwd=token,
            password="RYYxtwbl2Ig4Y1jV",
            port=PORT,
            database=DBNAME,
            ssl={'ca': 'global-bundle.pem'}
        )

        # Step 1: Retrieve Question IDs
        cur = conn.cursor()
        select_questions_query = """
            SELECT id
            FROM questions
            WHERE category = %s AND answer_date IS NOT NULL
            ORDER BY answer_date DESC
            LIMIT 20
        """
        cur.execute(select_questions_query, (category,))
        question_ids = [row[0] for row in cur.fetchall()]
        print(question_ids)
        # Step 2: Retrieve User IDs
        select_users_query = """
            SELECT user_id
            FROM user_cats
            WHERE category_id = %s
        """
        cur.execute(select_users_query, (category,))
        user_ids = [row[0] for row in cur.fetchall()]
        print(user_ids)

        # Step 3: Calculate Scores
        scores = {}
        for user_id in user_ids:
            select_score_query = """
                SELECT SUM(difficulty)
                FROM answers
                WHERE question_id IN %s AND user_id = %s AND result = 1
            """
            cur.execute(select_score_query, (question_ids, user_id))
            score = cur.fetchone()[0] or 0
            scores[user_id] = score
        print(scores);
        # Step 4: Store Scores
        for user_id, score in scores.items():
            update_score_query = """
                UPDATE user_cats
                SET score = %s
                WHERE user_id = %s AND category_id = %s
            """
            cur.execute(update_score_query, (score, user_id, category))

        # Step 5: Sort Users by Score
        sorted_users = sorted(user_ids, key=lambda user_id: scores[user_id], reverse=True)
        print(sorted_users);
        # Step 6: Calculate Percentile Rank
        # num_users = len(sorted_users)
        # for i, user_id in enumerate(sorted_users, start=1):
        #     cur.execute("SET @rank := 0, @prev_score := NULL")
        #     percentile_rank_query = """
        #         SELECT user_id, score, 
        #         CASE
        #             WHEN @prev_score = score THEN @rank
        #             WHEN @prev_score := score THEN @rank := @rank + 1
        #         END AS percentile_rank
        #         FROM (
        #             SELECT user_id, score
        #             FROM user_cats
        #             WHERE category_id = %s
        #             ORDER BY score DESC
        #         ) AS ranked
        #         WHERE user_id = %s
        #     """
        #     cur.execute(percentile_rank_query, (category, user_id))
        #     row = cur.fetchone()
        #     user_id, score, rank = row[0], row[1], row[2]
        #     print(user_id)
        #     print(score)
        #     print(rank)
        # 
        #     # Calculate percentile rank
        #     percentile_rank = ((num_users - rank + 1) / num_users) * 100
        #     # Step 7: Store Percentile Rank
        #     update_percentile_rank_query = """
        #         UPDATE user_cats
        #         SET percentile_rank = %s
        #         WHERE user_id = %s AND category_id = %s
        #     """
        #     cur.execute(update_percentile_rank_query, (percentile_rank, user_id, category))
        # Step 6: Calculate Percentile Rank
        
        num_users = len(sorted_users)
        current_rank = 0
        current_score = None

        for i, user_id in enumerate(sorted_users, start=1):
            # Calculate percentile rank
            if scores[user_id] != current_score:
                print("user id: {}".format(user_id))
                print("score: {}".format(scores[user_id]))
                # If the user's score is different from the previous user's score,
                # update the current_rank and current_score
                current_rank = i
                current_score = scores[user_id]

            percentile_rank = (num_users - current_rank + 1) / num_users * 100
            print("current rank: {}".format(current_rank))
            print("percentile rank: {}".format(percentile_rank))

            print()
            # Update the user's percentile rank in the user_cats table
            update_percentile_rank_query = """
                UPDATE user_cats
                SET percentile_rank = %s
                WHERE user_id = %s AND category_id = %s
            """
            cur.execute(update_percentile_rank_query, (percentile_rank, user_id, category))

        conn.commit()
        conn.close()
        return {
            'statusCode': 200,
            'body': "Process completed successfully"
        }

    except Exception as e:
        print("Database connection or processing failed due to {}".format(e))
        return "Failed to process data"
