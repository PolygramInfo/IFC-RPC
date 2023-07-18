import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any
import hashlib
from http import HTTPStatus

import boto3
from botocore import exceptions
from cloudevents.conversion import to_binary, to_structured, to_json, from_dict
from cloudevents.http import CloudEvent

from .event_authenticator import get_user, validate_token, authorize_user
from .event_validator import validate_event

"""
The ambassador is the "frontdoor" to the client. 
This is where the client will send their events to and the
internal operation that will produce and register a new
resource.

Methods:
    __init__ - Initialize the ambassador
    authenticate - Authenticate the client
    validate - Validate the event
    generate_resource - Generate a new resource
    register_resource - Register the new resource
    log_event - Log the event

This service has three possible responses:
    1. 200 - The event was successfully processed
    2. 400 - The event was not processed due to an invalid event type
    3. 500 - The event was not processed due to an internal error

Example request:
    {
        "type": "sampletype1",
        "data": {  
            "test": "data"
        }
    } # TODO: Make this correct

Example response:
    {
        "statusCode": 200,
        "message": "Event successfully processed",
        "data": {
            "resource_uri": "https://api.com/resource/1234567890",
            "resource_id": "1234567890",
            "transaction_id": "1234567890"
            "try_after": "2020-01-01T00:00:00Z"
        }
    }
"""    

class Ambassador:

    def __init__(self):
        """
        Initialize the ambassador
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(stream_handler)

        try:
            self.session = boto3.Session()
        except exceptions.NoCredentialsError:
            self.logger.error("No AWS Credentials found. Logging to CloudWatch will not be available.")
            raise exceptions.NoCredentialsError("No AWS Credentials found. Logging to CloudWatch will not be available.")

        self.dynamodb = self.session.resource("dynamodb")
        self.s3 = self.session.resource("s3")
    
    def authenticate(self, user_hash:str, token:str)->int:
        """
        Authenticate the client
        """
        return authorize_user(user_hash, token)
    
    def validate_event(self, event):
        """
        Validate the event conforms to event schema this step
        does not check the data of the event, only the event is
        a valid event type and has the required fields.
        """
        return validate_event(event)
    
    def generate_resource(self, event)->dict:
        """
        Generate a new resource
        """
        resource_id = uuid.uuid4().hex

        return {
            "resource_id": resource_id
        }

    def register_resource(self, resource:dict)->bool:
        """
        Register the new resource
        """
        table = self.dynamodb.Table("Resources")

        response = table.put_item(
            Item={
                "resource_id": resource["resource_id"],
                "updated_at": datetime.utcnow().isoformat(),
                "created_on": datetime.utcnow().isoformat(),
                "expires_after": int((datetime.utcnow() + 
                                  timedelta(
                                    hours=6))
                                    .timestamp()),
                "published": False,
            },
            ConditionExpression="attribute_not_exists(resource_id)"
        )

        #TODO do something here to log the resource.

        self.logger.debug(f"Response from DynamoDB: {response}")

        if response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK:
            return True
        else:
            return False
        
    def log_event(self, event:CloudEvent, resource:dict, resource_status:bool)->None:
        """
        Log the event
        """
        s3_bucket = self.s3.Bucket("pg-poc-logging") # Remove hardcoded bucket name
        bucket_prefix = f"{datetime.now().date().isoformat()}/{datetime.now().hour}"
        s3_key = f"{s3_bucket}/{bucket_prefix}/{event.get('id', uuid.uuid4())}.json"

        body = {
            "metadata": {},
            "event": json.dumps(event),
            "resource": json.dumps(resource),
            "resource_created": str(resource_status)
        }

        s3_bucket.put_object(
            Body=json.dumps(body),
            Key=f"/{datetime.now().date()}/{s3_key}"
        )

    def forward_event(self, event, resource_id):
        """
        Forward the event to the next service
        """
        sqs = self.session.resource("sqs")
        queue = sqs.get_queue_by_name(QueueName="validator-queue")

        event["resource_id"] = resource_id
        cloud_event = from_dict(CloudEvent, event)

        try:
            response = queue.send_message(
                MessageBody=to_json(cloud_event).decode("utf-8"),
                MessageAttributes={}
            )
            logging.debug(f"Response from SQS: {response}")
            logging.info(f"Event sent to validation SQS")
        except boto3.exceptions.botocore.exceptions.ClientError as e:
            self.logger.error(f"Error sending message to SQS: {e}")
            self.logger.debug(f"Event: {cloud_event}")
            raise e
        
        return None

    def generate_response(
            self, 
            event_id:str, 
            resource:dict=None, 
            status_code:int=200,
            transaction_id:str=None,
            message:str=None )->dict:

        attributes = {
            "id": event_id,
            "type": "com.ambassador.event.response",
            "source": "https://lambda.us-west-2.amazonaws.com/2015-03-31/functions/event_ambassador/invocations",
            "transactionid": transaction_id,
        }

        if status_code >= 299:
            attributes["type"] = "com.ambassador.event.error"

        if resource:
            data = resource

        if message:
            data = {
                "error_message": message
            }

        cloud_event = CloudEvent(
            attributes=attributes,
            data=data
        )

        lambda_reply = {
            "headers": {
                "content-type": "application/json"
            },
            "statusCode": status_code,
            "body":to_json(cloud_event).decode("utf-8")
        }

        return lambda_reply

    def main(self, event)->dict:
        """
        Main function
        """

        self.logger.info("Received event: %s", event)

        # Authenticate the client

        auth_response = self.authenticate(event["userhash"], event["usertoken"])

        if not auth_response["status"]:
            self.logger.error("Unable to authenticate user")
            self.logger.debug(f"Auth response: {auth_response}")
                
            return self.generate_response(
                event["id"], 
                status_code=auth_response["http_status"].conjugate(), 
                message=auth_response["message"])
        
        # Validate the event
        if not self.validate_event(event):
            self.logger.error("Invalid event")
            return self.generate_response(
                event["id"], 
                status_code=HTTPStatus.BAD_REQUEST.conjugate(), 
                message="Event does not conform to event schema")

        # Generate a new resource
        resource = self.generate_resource(event)

        # Register the new resource
        response = self.register_resource(resource)

        # Log the event
        self.log_event(
            event, 
            resource, 
            response
        ) #TODO log the resource data as well.

        # Forward the event
        response = self.forward_event(
            event, resource["resource_id"])

        # Generate the response
        reply = self.generate_response(
            event["id"], 
            resource=resource, 
            status_code=HTTPStatus.CREATED.conjugate(),
            transaction_id=event["transactionid"]
        )

        return reply
    
def lambda_handler(event, context):
    """
    Lambda handler
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(stream_handler)

    return Ambassador().main(event)  

if __name__ == "__main__":
    import time

    session = boto3.session.Session()
    dynamodb = session.client("dynamodb")

    user_hash = hashlib.md5("test@test.com".encode("utf-8")).hexdigest()
    token = f"{uuid.uuid4().hex[0:32]}.{uuid.uuid4().hex[0:32]}"
    expiration = int((datetime.utcnow() + timedelta(hours=1)).timestamp())

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

    time.sleep(5)

    attr = {
        "id": uuid.uuid4().hex,
        "type": "com.ambassador.event.request",
        "source": "https://ambassador.example.com",
        "time": str(datetime.utcnow().isoformat()),
        "specversion": "1.0",
        "datacontenttype": "application/json",
        "user_hash": user_hash,
        "token": token,
        "transaction_id": "1234567890",
        "dataschema": "test_wall"
    }          

    data = {
        'id': 'bd9e7a2381564a2ca2ce36cc41b88a1c', 
        'describes': 'wall', 
        'subcomponents': {'dimensions': {'width': 10, 'height': 10, 'length': 10}
            }
        }

    event = CloudEvent(attributes=attr, data=data)

    print(Ambassador().main(event))



