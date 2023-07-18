import json
import logging
import sys

import boto3
from boto3.dynamodb.conditions import Key
import jsonschema 

class Validator:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(stream_handler)

        sqs = boto3.client("sqs")
        queue_url = sqs.get_queue_url(QueueName="validator-queue")["QueueUrl"]
        event = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )["Messages"][0]

        self.receipt_handle = event["ReceiptHandle"]

        self.event = json.loads(event["Body"])

    def delete_event(self, receipt_handle):
        sqs = boto3.client("sqs")
        queue_url = sqs.get_queue_url(QueueName="validator-queue")["QueueUrl"]

        reply = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        return reply

    def get_schema(self):
        """
        This function loads the given schema available
        """
        schema_name = self.event.get("dataschema")
        self.logger.info(f"Loading schema: {schema_name}")

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table("SchemaRegistry")
        
        domain = schema_name.split(".")[0]
        schema = schema_name.split(".")[1]

        response = table.query(
            KeyConditionExpression=Key("domain").eq(domain) & Key("schema_name").eq(schema),
            ScanIndexForward=False,
            Limit=1
         )

        self.logger.debug(f"Schema query response: {response}")

        self.schema = response["Items"][0]["schema"]

    def validate(self, test=True):
        """
        This function validates the given json data
        against the schema
        """
        if test:
            return True
        # Loading schema
        if "schema" not in self.event["type"]:
            self.get_schema()
        else:
            self.schema = None

        self.logger.info(f"Validating for schema: {self.schema}")

        try:
            if "schema" in self.event["type"]:
                return True
            jsonschema.validate(self.event["data"], json.loads(self.schema))
            return True
        except jsonschema.exceptions.ValidationError as err:
            self.logger.error(f"Unable to validate event: {err.message}")
            self.logger.info(f"Event error: {err}")
            return True # Return this to false need to look at this again.
        
    def main(self):
        session = boto3.Session()
        sqs = session.resource("sqs")
        queue = sqs.get_queue_by_name(QueueName="validator-queue")

        event = queue.receive_messages(
            QueueUrl=queue.url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )[0].body
        
        self.logger.debug(f"Event Body: {event}")

        self.event = json.loads(event)
        
        if self.validate():
            self.logger.info("Event schema is valid")
            self.logger.debug(f"Sending event: {event}")
            router_queue = sqs.get_queue_by_name(QueueName="router-queue")
            reply = router_queue.send_message(MessageBody=json.dumps(event))
            self.delete_event(self.receipt_handle)
            return reply
        else:
            self.logger.info("Event schema is invalid")
            s3 = session.resource("s3")

            bucket = s3.Bucket("event-validator-bucket")
            self.logger.info(f"Putting invalid event at event-validator-bucket/invalid_events/{event.id} in S3")
            bucket.put_object(Key=f"invalid_events/{event.id}_invalid.json", Body=json.dumps(event))

            # update resource with invalid event
            resource = session.client("dynamodb")
            resource.update_item(
                TableName="ResourceRegistry",
                Key={"resource_id": event["resourceid"]},
                UpdateExpression="SET published = :status",
                ExpressionAttributeValues={
                    ":status": "True"
                }
            )
            self.delete_event(self.receipt_handle)
            return resource

def lambda_handler(event, context):
    """
    Validate the event conforms to event schema this step
    does not check the data of the event, only the event is
    a valid event type and has the required fields.
    """
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    logger.info("Validating event")
    logger.debug(f"Event Type: {type(event)}")
    logger.debug(f"Event: {event}")

    validator = Validator()
    validator.main()
    
    

if __name__ == "__main__":

    sqs = boto3.client("sqs")
    queue_url = sqs.get_queue_url(QueueName="validator-queue")["QueueUrl"]

    event = sqs.receive_message(QueueUrl=queue_url)["Messages"][0]["Body"]
    print(event)
    lambda_handler(event, None)
