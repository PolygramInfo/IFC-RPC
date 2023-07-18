import boto3
import hashlib
import uuid
from datetime import datetime
from datetime import timedelta

dynamodb = boto3.client("dynamodb")
user_hash = hashlib.sha256("zach@polygram.info".encode("utf-8")).hexdigest()
token = f"{uuid.uuid4().hex[0:32]}.{uuid.uuid4().hex[0:32]}"
expiration = int((datetime.utcnow() + timedelta(days=30)).timestamp())

dynamodb.update_item(
    TableName="Users",
    Key={
        "user_hash": {
            "S": user_hash
        }
    },
    UpdateExpression="SET #token = :val1, #expiration_timestamp = :val2, #created = :val3",
    ExpressionAttributeNames={
        "#token": "token",
        "#expiration_timestamp": "expiration_timestamp",
        "#created": "created"
    },        
    ExpressionAttributeValues={
        ":val1": {
            "S": token
        },
        ":val2": {
            "N": str(expiration)
        },
        ":val3": {
            "N": str(int(datetime(2023, 7, 9).timestamp()))
    }
})