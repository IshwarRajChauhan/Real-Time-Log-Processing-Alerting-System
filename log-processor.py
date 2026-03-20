import json
import base64
import boto3
import os
from datetime import datetime

# AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Environment variables
TABLE_NAME = os.environ['TABLE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):

    for record in event['Records']:
        try:
            # 🔹 Decode Kinesis data
            payload = base64.b64decode(record['kinesis']['data'])
            log = json.loads(payload)

            # 🔹 Extract fields
            level = log.get('level')
            latency = log.get('latency')
            service = log.get('service')
            timestamp = log.get('timestamp')

            # 🔹 Current time for partitioning
            now = datetime.utcnow()

            # 🔥 Partitioned S3 key (IMPORTANT)
            s3_key = (
                f"year={now.year}/"
                f"month={str(now.month).zfill(2)}/"
                f"day={str(now.day).zfill(2)}/"
                f"hour={str(now.hour).zfill(2)}/"
                f"log-{context.aws_request_id}.json"
            )

            # 🔹 Store ALL logs in S3
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(log)
            )

            # 🔥 Anomaly Detection Logic
            is_error = level == "ERROR"
            is_slow = latency and latency > 1000

            if is_error or is_slow:

                issue_type = "ERROR" if is_error else "HIGH_LATENCY"

                # 🔹 Store in DynamoDB
                table.put_item(
                    Item={
                        "log_id": context.aws_request_id,
                        "timestamp": timestamp,
                        "service": service,
                        "issue_type": issue_type
                    }
                )

                # 🔹 Send SNS Alert
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=f"🚨 Alert: {issue_type} in {service} | {log}"
                )

        except Exception as e:
            print("Error processing record:", str(e))

    return {
        "statusCode": 200,
        "body": json.dumps("Processing complete")
    }