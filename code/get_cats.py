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
        # Extract language and user_id from the event body
        body = json.loads(event.get('body', '{}'))
        language = body.get('language')
        user_id = body.get('user_id')

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

        # Retrieve categories with default=1 and the specified language
        cur = conn.cursor()
        select_default_category_query = """
            SELECT *
            FROM categories
            WHERE `default` = 0 AND language = %s AND status = 1
        """
        cur.execute(select_default_category_query, (language,))
        default_category_results = cur.fetchall()

        # Format the result as a list of dictionaries for default category
        default_category_list = []
        for row in default_category_results:
            category = {
                'id': row[0],
                'name': row[1],
                'language': row[2],
                'status': row[3],
                'default': row[4]
            }
            default_category_list.append(category)

        # Retrieve all categories with the given language
        select_all_query = "SELECT * FROM categories WHERE language = %s AND status = 1"
        cur.execute(select_all_query, (language,))
        all_categories_results = cur.fetchall()

        # Format the result as a list of dictionaries for all categories
        all_categories_list = []
        for row in all_categories_results:
            category = {
                "id": row[0],
                "name": row[1],
                "language": row[2],
                "status": row[3],
                "default": row[4]
            }
            all_categories_list.append(category)

        # If user_id is provided, retrieve user-specific categories
        user_categories_list = []
        if user_id:
            select_user_categories_query = """
                SELECT c.id, c.name, c.language, c.status, c.`default`
                FROM categories AS c
                JOIN user_cats AS uc ON c.id = uc.category_id
                WHERE uc.user_id = %s AND c.language = %s AND c.status = 1
            """
            cur.execute(select_user_categories_query, (user_id, language))
            user_categories_results = cur.fetchall()

            # Format the result as a list of dictionaries for user-specific categories
            for row in user_categories_results:
                category = {
                    'id': row[0],
                    'name': row[1],
                    'language': row[2],
                    'status': row[3],
                    'default': row[4]
                }
                user_categories_list.append(category)

        conn.close()

        # Return all lists as JSON
        return {
            "all_categories": all_categories_list,
            "default_category": default_category_list,
            "user_categories": user_categories_list
        }

    except Exception as e:
        print("Database connection failed due to {}".format(e))
        return "Failed to retrieve categories"
