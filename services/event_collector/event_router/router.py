import boto3
import enum
import json
import logging
import sys

from cloudevents.http import CloudEvent
from cloudevents.conversion import from_dict, to_dict
import cloudevents.exceptions

class Router:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(stream_handler)

        sqs = boto3.client("sqs")
        queue_url = sqs.get_queue_url(QueueName="router-queue")["QueueUrl"]
        event = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )["Messages"][0]
        
        self.logger.debug(f"Received Event: {event}")

        self.receipt_handle = event["ReceiptHandle"]

        self.logger.debug("Received event from queue")
        self.logger.debug(f"Event:\n {json.dumps(event)}")
        self.logger.info("Interpolating CloudEvent.")
        try:
            self.event = from_dict(CloudEvent, json.loads(json.loads(event["Body"])))
        except cloudevents.exceptions.MissingRequiredFields as err:
             self.logger.error("Unable to create event due to malformed event payload.")
             self.logger.error(f"Error:\n {err}")
             self.logger.debug(f"Invalid event payload:\n {json.dumps(to_dict(self.event))}")
             raise err
        except Exception as err:
             self.logger.error("Unable to build event for unknown reason.")
             self.logger.error(f"Error:\n {err}")
             raise err

    def delete_message(self, receipt_handle):
        sqs = boto3.client("sqs")
        queue_url = sqs.get_queue_url(QueueName="router-queue")["QueueUrl"]

        reply = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        return reply


    def route_event(self):
        sqs = boto3.client("sqs")

        reply = {}
        event_type = self.event.get_attributes()["type"].split("/")[0]
        if event_type == "com.pg.entity":
            self.logger.info(f"Routing event {self.event.get_attributes()['id']} to data-queue")
            reply = sqs.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/587248761314/data-queue",
                MessageBody=json.dumps(to_dict(self.event))
            )
        if event_type == "com.pg.schema":
            reply = sqs.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/587248761314/schema-queue", 
                MessageBody=json.dumps(to_dict(self.event))
            )
        else:
            reply = {"message":"No route to service."}

        reply["sentEvent"]:json.dumps(to_dict(self.event))
        return reply

    def main(self):
        reply = self.route_event()
        self.delete_message(self.receipt_handle)
        return reply

def lambda_handler(event, context):
    """
    This is the entry point for the lambda function. 
    """

    sessions = boto3.Session()
    sqs = sessions.client("sqs")
    event = from_dict(json.loads(event["body"]))

    Router().main()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world"
        })
    }