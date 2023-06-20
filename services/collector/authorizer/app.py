import os
import json
from datetime import (
    datetime, 
    timedelta
)
from dataclasses import dataclass
import uuid

# AWS libraries
import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3 import exceptions

# Third Party Library
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured, to_binary

# First Party Library
@dataclass
class Response:
    event_id: str
    transaction_id: str
    resource_id: str = None
    retry_after: str = None

    def __post_init__(self):
        self.__dict__ = {
            "source_event_id": self.event_id,
            "transaction_id": self.transaction_id,
            "resource_id": self.resource_id,
            "retry_after": self.retry_after
        }


dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
sqs = boto3.client('sqs', region_name="us-east-1")

def authorize_user(user_hash:str=None, token:str=None)->dict:
    if not user_hash or not token:
        return {"StatusCode": 500, "message": "No user or token provided."}

    table = dynamodb.Table("UserToken")

    response = table.query(
        KeyConditionExpression=Key("userhash").eq(user_hash)
    )

    print(response)
    for item in response["Items"]:
        if datetime.fromtimestamp(int(item.get("expiration_ts"))) <= datetime.now():
            return {"StatusCode": 403, "message": f"Expired token."}
        if not item or token not in item.get("token"):
            return {"StatusCode": 403, "message": f"Invalid user or token."}
        if token != item.get("token"):
            return {"StatusCode": 403, "message": f"Invalid user or token"}
    
    return {"StatusCode": 200, "message": None}

def send_sqs_message(message_body:dict, message_attributes:dict=None):

    if not message_attributes:
        message_attributes = {}


    response = sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/587248761314/collector-event-queue",
        DelaySeconds=10,
        MessageAttributes=message_attributes,
        MessageBody=message_body
    )

    return response
    
def lambda_handler(event, context):

    if not event:
        return {"statusCode":500, "headers":{}, "body":"No event body."}
    
    if isinstance(event, str):
        event_body = json.loads(event)
    else:
        event_body = event
    
    #print(type(json.loads(event_body)))

    if not event_body:
        return {"statusCode":500, "headers":{}, "body":"No event body."}

    data = event_body.pop("data")

    attributes = {
        "type": "com.example.response",
        "content-type": "application/cloudevents-bulk+json",
        "source": "https://example.com/ifc/event/authorizer"
        }

    auth_response = authorize_user(
        user_hash=event_body.get("userhash", None), 
        token=event_body.get("token", None)
    )

    
    if auth_response.get("StatusCode") >= 300:

        response = Response(
            event_id=event_body["id"],
            transaction_id=event_body.get("transactionid",None)
        ).__dict__
        
        response["message"] = auth_response.get("message",None)
        
        headers, body = to_structured(CloudEvent(attributes=attributes, data=response))
        return {
            "statusCode": auth_response.get("StatusCode"),
            "headers": headers,
            "body": body
        }

    response = Response(
        event_id=event_body.get("id"),
        transaction_id=event_body.get("transactionid",None),
        resource_id=uuid.uuid4().hex,
        retry_after=str(datetime.now() + timedelta(seconds=30))
    )
    
    send_sqs_message(
        message_attributes={},
        message_body=json.dumps({"transaction":response.__dict__,"event":event_body})
    )

    headers, body = to_structured(CloudEvent(attributes=attributes, data=response.__dict__))

    return {
        "statusCode": auth_response.get("StatusCode"),
        "headers": headers,
        "body": body
    }

if __name__ == "__main__":
    from hashlib import md5

    attributes = {
        "type": "com.example.sampletype1",
        "content-type": "application/cloudevents-bulk+json",
        "source": "https://example.com/event-producer",
        "dataschema": "http://myrepository.com/data-schema",
        "userhash": "123abc",
        "token": "abcdefg"
    }  

    data = {
        "transaction_id": uuid.uuid4().hex,
        "somedata": True
    }

    headers, data = to_structured(CloudEvent(attributes=attributes, data=json.dumps(data)))

    event = {
        "body": data
    }

    result = lambda_handler(event=event, context={})

    print(result)