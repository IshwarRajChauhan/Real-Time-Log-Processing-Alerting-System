import boto3
import json
import random
import time
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='ap-south-1')

STREAM_NAME = "log-stream"

services = ["auth", "payment", "orders"]

def generate_log():
    return {
        "timestamp": str(datetime.utcnow()),
        "service": random.choice(services),
        "level": random.choice(["INFO", "ERROR"]),
        "latency": random.randint(100, 2000)
    }

while True:
    log = generate_log()

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(log),
        PartitionKey="key1"
    )

    print("Sent:", log)
    time.sleep(1)