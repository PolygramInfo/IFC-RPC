import os
import json
from datetime import (
    datetime, 
    timedelta
)
from dataclasses import dataclass, field
from typing import Any
import uuid
from enum import Enum
import logging
# AWS SDK
import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3 import exceptions

# Third Party Library
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured, to_binary

# Local library
import sdk

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

logger.addHandler(stream_handler)

try:
    session = boto3.Session()
    cloudwatch_logs = session.client("logs")

    cloudwatch_handler = logging.StreamHandler()
    cloudwatch_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    cloudwatch_handler.setLevel(logging.INFO)

    cloudwatch_handler.setStream(
        cloudwatch_logs.create_log_stream(
        logGroupName="collector",
        logStreamName="collector_stream")["logStreamName"]
    )

    logger.addHandler(cloudwatch_handler)
except exceptions.NoCredentialsError:
    logger.warning("No AWS Credentials found. Logging to CloudWatch will not be available.")

class Services(Enum):
    DATA_MANAGER = "$DatamanagerURI" #TODO: Replace this with a real URI

@dataclass
class LambdaReturn:
    statusCode:int
    headers:dict = field(default_factory=dict())
    body:dict = field(default_factory=dict())

    def __post_init__(self):
        self.__dict__ = {
            "statusCode": self.StatusCode,
            "headers": self.headers,
            "body": json.dumps(self.body)
        }

    def __setattr__(self, __name: str, __value: Any) -> None:
        self.__setattr__(__name=__name, __value=__value)

        self.__dict__[__name] = __value if __name != "body" else json.dumps(__value)

    def __str__(self) -> str:
        return(json.dumps(self.__dict__))

region_name = "us-east-1"
sqs_client = boto3.client("sqs", region_name = region_name)
dynamo_client = boto3.client("dynamodb", region_name = region_name)
s3_client = boto3.client("s3", region_name=region_name)

def log_event(event:dict, bucket_name:str=os.environ["LOG_BUCKET"])->int:
    
    s3_client.put_object(
        Body=json.dumps(event),
        Bucket=os.environ["EVENT_LOG_BUCKET"],
        Key=f"event_logs/{event.get('id')}.json"
    )

def route_event(event:dict, event_type:str):

    message_attributes = {}

    response = sqs_client.send_message(
        QueueUrl=Services[event_type.upper()].value,
        DelaySeconds=0,
        MessageAttributes=message_attributes,
        MessageBody=json.dumps(event),
        MessageGroupId='Event'
    )

def authorize_user(user_hash:str, token:str)->int:
    if not user_hash or not token:
        return {"StatusCode": 500, "message": "No user or token provided."}

    table = dynamo_client.Table("UserToken")

    response = table.query(
        KeyConditionExpression=Key("userhash").eq(user_hash)
    )

    print(response)
    for item in response["Items"]:
        if datetime.fromtimestamp(int(item.get("expiration_ts"))) <= datetime.now():
            return {"statusCode": 403, "message": f"Expired token."}
        if not item or token not in item.get("token"):
            return {"statusCode": 403, "message": f"Invalid user or token."}
        if token != item.get("token"):
            return {"statusCode": 403, "message": f"Invalid user or token"}
    
    return {"statusCode": 200, "message": None}

def handler(event, context):
    
    response = LambdaReturn(
        StatusCode=503,
        headers={"Content-Type": "text/plain"},
        body=json.dumps({"message": "Service Unavailable"})
    )

    try:
        authorizer_response = authorize_user(
            user_hash=event.get("userhash"),
            token=event.get("token")
        )

        if authorizer_response.get("StatusCode") != 200:
            response.statusCode = authorizer_response.get("StatusCode")
            response.body = json.dumps({"message": authorizer_response.get("message")})
            return response.__dict__
        
        response.body["message"] = "Event received."
        response.body["resource_id"] = uuid.uuid4().hex
        response.statusCode = 200
    except Exception as e:
        response.body["message"] = f"Error: {e}"
        response.statusCode = 500
        return response.__dict__



    response.body = json.dumps(event)
    
    return response.__dict__