import logging
import enum
import json
import hashlib

import boto3
from boto3 import exceptions

def create_logger()->logging.Logger:
    # Set logging for this module
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    logger.addHandler(stream_handler)

    try:
        session = boto3.Session()
        cloudwatch_logs = session.client("logs")

        cloudwatch_handler = logging.StreamHandler()
        cloudwatch_handler.setLevel(logging.INFO)
        cloudwatch_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        cloudwatch_handler.setStream(
            cloudwatch_logs.create_log_stream(
                logGroupName="collector",
                logStreamName="collector_stream"
            )["logStreamName"]
        )

        logger.addHandler(cloudwatch_handler)
    except exceptions.NoCredentialsError:
        logger.warning("No AWS Credentials found. Logging to CloudWatch will not be available.")   

    return logger

logger = create_logger()

class EventTypes(enum.Enum):
    """
    Enumerates the different types of events that can be sent to the system.
    """
    sampletype1 = "sampletype1"
    sampletype2 = "sampletype2"
    sampletype3 = "sampletype3"

def send_message_to_sqs(
        uri:str, 
        event:dict=None, 
        resource_id:str=None, 
        transaction_id:str=None
    )->dict:
    """
    Sends a message to an SQS queue.

    Example call: send_message_to_sqs(
        uri = "https://sqs.us-east-1.amazonaws.com/123456789012/queue_name", 
        event = {"type": "sampletype1", "data": {"key": "value"}}, 
        resource_id = "d2b7a9d5-3d7e-4f3e-9f7c-8c5d1e2a7b6d" , 
        transaction_id = "f6d1d3d4-2d1c-4a5b-9c8d-0e7f6a5b4c3e")
    """

    if not uri:
        logger.info("No URI provided.")
        return {"Error": "No URI provided."}
    
    if not event:
        logger.info("No event provided.")
        return {"Error": "No event provided."}
    
    if not resource_id:
        logger.info("No resource ID provided.")

    if not transaction_id:
        logger.info("No transaction ID provided.")

    try:
        session = boto3.Session()
        sqs = session.client("sqs")
    except exceptions.NoCredentialsError:
        logger.error("No AWS Credentials found.")
        raise exceptions.NoCredentialsError
    
    unique_event_id = hashlib.sha256(event.get("data")).hexdigest()

    response = sqs.send_message(
        QueueUrl = uri,
        MessageBody = json.dumps(event.get("data")),
        MessageDedupeId = unique_event_id,
        MessageGroupId = transaction_id,
        MessageAttributes = {
            "resource_id": {
                "StringValue": resource_id,
                "DataType": "String"
            },
            "transaction_id": {
                "StringValue": transaction_id,
                "DataType": "String"
            },
            "unique_event_id": {
                "StringValue": unique_event_id,
                "DataType": "String"
            },
            "event_type": {
                "StringValue": event.get("type"),
                "DataType": "String"
            }
        }
    )

    return response

def route_event(event:dict=None, resource_id:str=None, transaction_id:str=None)->int:

    if not event:
        logger.info("No event provided.")
        return 500
    
    if not resource_id:
        logger.info("No resource ID provided.")
        return 500
    
    if event.get("type") not in EventTypes.__members__:
        logger.info(f"Event type {event.get('type')} not a supported service.")
        return 400

    session = boto3.Session()
    sqs = session.client("sqs")

    unique_event_id = hashlib.sha256(event.get("data")).hexdigest() # This is used to deduplicate messages in SQS

    try:
        if event.get("type") == EventTypes.sampletype1:
            response = send_message_to_sqs(
                uri=EventTypes.sampletype1.value,
                event=event,
                resource_id=resource_id,
                transaction_id=transaction_id
            )
        elif event.get("type") == EventTypes.sampletype2:
            response = send_message_to_sqs(
                uri=EventTypes.sampletype2.value,
                event=event,
                resource_id=resource_id,
                transaction_id=transaction_id
            )
        elif event.get("type") == EventTypes.sampletype3:
            response = send_message_to_sqs(
                uri=EventTypes.sampletype3.value,
                event=event,
                resource_id=resource_id,
                transaction_id=transaction_id
            )
        else:
            logger.info(f"Event type {event.get('type')} not a supported service.")
            return 400
    except exceptions.ClientError as e:
        logger.error(f"Error sending message to SQS: {e}")
        return 500
    
    logger.info(f"Message sent to SQS: {response}")

    if response.get("Error"):
        logger.error(f"Error sending message to SQS: {response}")
        return 500
    
    if response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
        logger.error(f"Error sending message to SQS: {response}")
        return 500 # Internal Server Error

    return 200 # Success

    