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
from cloudevents.conversion import to_binary, to_structured
from cloudevents.http import CloudEvent

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
        self.logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(stream_handler)

        try:
            self.session = boto3.Session(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region_name=os.environ["AWS_REGION"]
            )
        except exceptions.NoCredentialsError:
            self.logger.error("No AWS Credentials found. Logging to CloudWatch will not be available.")
            raise exceptions.NoCredentialsError("No AWS Credentials found. Logging to CloudWatch will not be available.")

        self.dynamodb = self.session.resource("dynamodb")
        self.s3 = self.session.resource("s3")
    
    def authenticate(self, user_hash:str, token:str)->int:
        """
        Authenticate the client
        """
        import authenticator
        return authenticator.authorize_user(user_hash, token)
    
    def validate_event(self, event):
        """
        Validate the event conforms to event schema this step
        does not check the data of the event, only the event is
        a valid event type and has the required fields.
        """
        import event_validator
        return event_validator.validate_event(event)
    
    def generate_resource(self, event)->dict:
        """
        Generate a new resource
        """
        resource_uri = os.environ["BASE_RESOURCE_URI"]
        resource_id = uuid.uuid4().hex

        return {
            "resource_uri": resource_uri + resource_id,
            "resource_id": resource_id
        }

    def register_resource(self, resource:dict)->dict:
        """
        Register the new resource
        """
        table = self.dynamodb.Table(os.environ["RESOURCE_TABLE_NAME"])

        response = table.put_item(
            Item={
                "resource_id": resource["resource_id"],
                "created_on": datetime.utcnow().isoformat(),
                "expires_after": (datetime.utcnow() + 
                                  timedelta(
                                    hours=int(os.environ["RESOURCE_LIFESPAN"]))).isoformat()
            },
            ConditionExpression="attribute_not_exists(resource_id)",
            ReturnValues="ALL_NEW"
        )

        #TODO do something here to log the resource.

        return response["Attributes"]

    def log_event(self, event):
        """
        Log the event
        """
        s3_bucket = self.s3.Bucket(os.environ["EVENT_BUCKET_NAME"])
        s3_key = f"{os.environ['EVENT_BUCKET_KEY_PREFIX']}_{event.get('id', uuid.uuid4())}.json"

        s3_bucket.put_object(
            Body=json.dumps(event),
            Key=f"/{datetime.now().date()}/{s3_key}"
        )

    def forward_event(self, event):
        """
        Forward the event to the next service
        """
        sqs = self.session.resource("sqs")
        queue = sqs.get_queue_by_name(QueueName=os.environ["EVENT_QUEUE_NAME"])

        cloud_event = CloudEvent().from_dict(event)

        try:
            response = queue.send_message(
                MessageBody=to_binary(cloud_event),
                MessageAttributes={},
                MessageGroupId=cloud_event["type"],
                MessageDeduplicationId=hashlib.md5(f"{cloud_event['id']}_{cloud_event['source']}_{cloud_event['time']}").hexdigest()
            )
        except boto3.exceptions.botocore.exceptions.ClientError as e:
            self.logger.error(f"Error sending message to SQS: {e}")
            self.logger.debug(f"Event: {cloud_event.to_dict()}")
            raise e

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
            "source": os.environ["AMBASSADOR_URI"],
            "transaction_id": transaction_id,
        }

        if status_code >= 299:
            attributes["type"] = "com.ambassador.event.error"

        if message:
            data = {
                "error_message": message
            }

        if resource:
            data = resource

        cloud_event = CloudEvent(
            attributes=attributes,
            data=resource
        )

        lambda_reply = {
            "headers": {
                "content-type": "application/json"
            },
            "statusCode": status_code,
            "body":json.dumps(to_structured(cloud_event))
        }

        return lambda_reply

    def main(self, event)->dict:
        """
        Main function
        """

        self.logger.info("Received event: %s", event)

        # Authenticate the client

        with self.authenticate(event["user_hash"], event["token"]) as auth:
            if not auth["status"]:
                self.logger.error("Unable to authenticate user")
                self.logger.debug(f"Auth response: {auth}")
                    
                return self.generate_response(
                    event["id"], 
                    status_code=auth["status_code"].conjugate(), 
                    message=auth["message"])
        
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
        self.log_event(event)

        # Forward the event
        response = self.forward_event(event)

        # Generate the response
        reply = self.generate_response(
            event["id"], 
            resource=response, 
            status_code=HTTPStatus.CREATED.conjugate())

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

    attr = {
        "id": uuid.uuid4().hex,
        "type": "com.ambassador.event.request",
        "source": "https://ambassador.example.com",
        "time": datetime.utcnow().isoformat(),
        "specversion": "1.0",
        "datacontenttype": "application/json",
        "user_hash": "1234567890",
        "token": "1234567890",
        "transaction_id": "1234567890"
    }          

    data = {
        "test": "test"
    }

    event = CloudEvent(attributes=attr, data=data)

    print(Ambassador().main(event))



