import pymysql
import boto3
import os

ENDPOINT = "predictative-instance-1.cb9thpviu0kz.us-east-1.rds.amazonaws.com"
PORT = 3306
USER = "predictuser"
REGION = "us-east-1"
DBNAME = "predictdb"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
session = boto3.Session()
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)
sns_client = boto3.client('sns', region_name=REGION)

def lambda_handler(event, context):
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

        cur = conn.cursor()
        select_query = "SELECT id, name FROM categories"
        cur.execute(select_query)
        categories = cur.fetchall()
        conn.close()

        for category in categories:
            category_id = category[0]
            category_name = category[1]

            topic_name = f"category-{category_id}-notifications"
            response = sns_client.create_topic(Name=topic_name)
            topic_arn = response['TopicArn']

            print(f"Created SNS topic '{topic_name}' with ARN: {topic_arn}")

        return "SNS topics created successfully"

    except Exception as e:
        print("Database connection or SNS topic creation failed due to {}".format(e))
        return "Failed to create SNS topics"
