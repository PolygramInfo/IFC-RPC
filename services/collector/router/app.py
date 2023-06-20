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

sqs = boto3.client("sqs", region_name="us-east-1")

def send_sqs_message(queue, message_attributes, message_body):
    if not message_attributes:
        message_attributes = {}

    print(message_body)
    response = sqs.send_message(
        QueueUrl=queue,
        DelaySeconds=0,
        MessageAttributes=message_attributes,
        MessageBody=message_body,
        MessageGroupId='Event'
    )

    return response

def lambda_handler(event, context):
    
    data = event

    if "sampletype1" in data.get("event").get("type"):
        queue = "https://sqs.us-east-1.amazonaws.com/587248761314/events-sampletype-queue.fifo"
        
        response = send_sqs_message(
            queue=queue,
            message_attributes={},
            message_body=json.dumps(event)
        )

        print(response)