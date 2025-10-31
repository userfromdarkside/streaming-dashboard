import boto3
import json
import random
import time
import uuid
from faker import Faker
from dataclasses import dataclass, field
from botocore.exceptions import ClientError

s3_resource = boto3.resource('s3')
#s3_resource.create_bucket(Bucket='data-stream-dump',CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
bucket_name = 'localstreams3'
# check if the s3 bucket exists
def bucket_exists(bucket_name):
    try:
        s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        print(f"Bucket '{bucket_name}' does not exist or is inaccessible.")
        return False

faker = Faker()
currencies = ['USD', 'GBP', 'EUR']

@dataclass
class Transaction:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory= lambda: currencies[random.randint(0,len(currencies)-1)])
    amount: str = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        return dict(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount
            }
        )

def Producer(file_string):
    filename = file_string
    with open(filename, 'a') as file:
        json.dump(Transaction().serialize(), file)
        file.write(",\n")
    s3_resource.Bucket(bucket_name).upload_file(Filename=filename, Key=filename)

for i in range(20): # change to 100 when test in production
    Producer(f'transactions_{str(uuid.uuid4())}.json')
    print('iteration:', i)
    time.sleep(3)