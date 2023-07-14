import logging
import sys
import uuid
from dataclasses import dataclass
import json

import boto3
import botocore

from cloudevents.http import CloudEvent
from cloudevents.conversion import from_dict, to_dict

class SchemaManager:

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        try:
            self.dynamodb = boto3.client("dynamodb")
        except botocore.exceptions.ClientError as err:
            self.logger.error(f"Unable to connect to DynamoDB: {err}")
            raise err
        
    def create(self,
            schema_domain:str,
            schema_name:str,
            schema:dict)->None:

        response = self.dynamodb.put_item(
            TableName="SchemaRegistry",
            Item={
                "schema_domain": {"S": schema_domain},
                "schema_name": {"S": schema_name},
                "schema": {"M": schema}
            },
            ConditionExpression="attribute_not_exists(schema_id)",
            ReturnValues="ALL_OLD"
        )

        return response
    
    def read(self, schema_domain:str, schema_name:str)->dict:

        schema = self.dynamodb.get_item(
            TableName="SchemaRegistry",
            Key={
                "schema_domain": {"S": schema_domain},
                "schema_name": {"S": schema_name},
            })["Item"]

        return schema
    
def lambda_handler(event, context):

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    logger.info(f"Received event: {event}")

    schema_manager = SchemaManager()

    session = boto3.Session()
    sqs = session.client("sqs")
    queue_url = sqs.get_queue_url(QueueName="SchemaQueue")["QueueUrl"]
    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")
    table = dynamodb.Table("Resources")

    event = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1
    )

    event = from_dict(CloudEvent, event)

    if event.get_attributes()["type"] == "com.schema.create":
        logger.info(f"Creating schema: {event.get_data()}")

        response = schema_manager.create(
            schema_domain=event.get_data()["schema_domain"],
            schema_name=event.get_data()["schema_name"],
            schema=event.get_data()["schema"]
        )
    elif event.get_attributes()["type"] == "com.schema.read":
        logger.info(f"Reading schema: {event.get_data()['schema_domain']}.{event.get_data()['schema_name']}")

        response = schema_manager.read(
            schema_domain=event.get_data()["schema_domain"],
            schema_name=event.get_data()["schema_name"]
        )

    logger.info(f"Response: {response}")

    bucket = "pg-resource-registery"
    key = f"resources/{event.get_attributes()['resource_id']}.json"

    resource_response = table.update_item(
        Key={
            "resource_id": event.get_attributes()["resource_id"],
            "updated_at": event.get_attributes()["time"]
        },
        UpdateExpression="SET #published = :published, #resource_url = :resource_url",
        ExpressionAttributeNames={
            "#published": "published",
            "#resource_url": "resource_url"
        },
        ExpressionAttributeValues={
            ":published": True,
            ":resource_url": f"s3://{bucket}/{key}"
        })
    
    logger.info(f"Resource response: {resource_response}")
    if resource_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        logger.error(f"Unable to update resource: {resource_response}")
        raise Exception(f"Unable to update resource: {resource_response}")
    
    s3_response = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(response["Item"])
    )

    logger.info(f"S3 response: {s3_response}")
    if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        logger.error(f"Unable to upload resource: {s3_response}")

        resource_response = table.update_item(
            Key={
                "resource_id": event.get_attributes()["resource_id"],
                "updated_at": event.get_attributes()["time"]
            },
            UpdateExpression="SET #published = :published, #resource_url = :resource_url",
            ExpressionAttributeNames={
                "#published": "published",
                "#resource_url": "resource_url"
            },
            ExpressionAttributeValues={
                ":published": False,
                ":resource_url": None
            })

        raise Exception(f"Unable to upload resource: {s3_response}")