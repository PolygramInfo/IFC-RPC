import boto3
import enum
import json

from cloudevents.http import CloudEvent
from cloudevents.conversion import from_dict

def lambda_handler(event, context):
    """
    This is the entry point for the lambda function. 
    """

    sessions = boto3.Session()
    sqs = sessions.client("sqs")
    event = from_dict(json.loads(event["body"]))

    event_type = event.get_attributes()["type"].split("/")[0]

    if event_type == "com.pg.data":
        sqs.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue", #TODO Create a new queue for the data manager
            MessageBody=json.dumps(event.get_data()),
            MessageDeduplicationId=event.get_attributes()["id"],
            MessageAttributes={
                "resource_id": event.get_attributes()["resource_id"],
                "transaction_id": event.get_attributes()["transaction_id"],
            }
        )
    if event_type == "com.pg.schema":
        sqs.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue", #TODO Create a new queue for the schema manager
            MessageBody=json.dumps(event.get_data()),
            MessageDeduplicationId=event.get_attributes()["id"],
            MessageAttributes={
                "resource_id": event.get_attributes()["resource_id"],
                "transaction_id": event.get_attributes()["transaction_id"],
            }
        )


    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world"
        })
    }