import logging
import sys
import uuid
from dataclasses import dataclass
import json

import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
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

        Item = {
            "domain": schema_domain,
            "schema_name": schema_name,
            "schema": schema
        }

        ts=TypeSerializer()
        response = self.dynamodb.put_item(
            TableName="SchemaRegistry",
            Item=ts.serialize(Item)["M"] ,
            ReturnValues="ALL_OLD"
        )

        return response
    
    def read(self, schema_domain:str, schema_name:str)->dict:

        schema = self.dynamodb.get_item(
            TableName="SchemaRegistry",
            Key={
                "domain": {"S": schema_domain},
                "schema_name": {"S": schema_name},
            })["Item"]

        return schema
    
    def list(self, filter:str=None, get_schema:bool=False)->dict:
        def deserialize_dynamo_item(item:dict)->dict:
            ds = TypeDeserializer()
            return {k:ds.deserialize(v) for k,v in item.items()}

        self.logger.info(f"Running scan with filter: {filter}")

        attributes_to_get = ["domain", "schema_name"]
        if get_schema:
            attributes_to_get.append("schema") 

        schemata=[]
        if filter:
            response=self.dynamodb.scan(
                TableName="SchemaRegistry",
                FilterExpression=Key("domain").eq(filter),
                AttributesToGet=attributes_to_get
            )

            self.logger.info(f"Got response: {len(response['Items'])} for filter: {filter}")
            schemata = [deserialize_dynamo_item(r) for r in response["Items"]]
            return schemata
        
        response=self.dynamodb.scan(
            TableName="SchemaRegistry",
            AttributesToGet=attributes_to_get
        )

        self.logger.info(f"Got response: {len(response['Items'])}")
        schemata = [deserialize_dynamo_item(r) for r in response["Items"]]

        return schemata
    
def lambda_handler(event, context):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    schema_manager = SchemaManager()

    session = boto3.Session()
    sqs = session.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName="schema-queue")
    s3 = session.client("s3")
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table("Resources")

    in_event = queue.receive_messages(
        MaxNumberOfMessages=1
    )

    logger.debug(f"Got messages: {len(in_event)}")
    logger.debug(f"Got event: {in_event[0].body}")

    event = from_dict(CloudEvent,json.loads(in_event[0].body))

    if event.get_attributes()["type"] == "com.pg.schema/create":
        logger.info(f"Creating schema: {event.get_data()}")

        response = schema_manager.create(
            schema_domain=event.get_data()["schema_domain"],
            schema_name=event.get_data()["schema_name"],
            schema=event.get_data()["schema"]
        )
        
    elif event.get_attributes()["type"] == "com.pg.schema/read":
        logger.info(f"Reading schema: {event.get_data()['schema_domain']}.{event.get_data()['schema_name']}")

        response = schema_manager.read(
            schema_domain=event.get_data()["schema_domain"],
            schema_name=event.get_data()["schema_name"]
        )
    elif event.get_attributes()["type"] == "com.pg.schema/list":
        response = schema_manager.list(
            filter=event.get_data().get("filter",None)
        )
        logger.info(f"Listing schemata")
        logger.debug(f"Schema filter: {event.get_data().get('filter',None)}")
    else:
        response = {
            "statusCode":500,
            "message":"Invalid schema operation."
        }

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
        Body=json.dumps(response.get("Item") if "Item" in response else response)
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
    
    queue.delete_messages(
        Entries=[
            {"Id": in_event[0].message_id, "ReceiptHandle": in_event[0].receipt_handle}
        ]
    )

    return {
        "statusCode": 200,
        "Message": {
            "s3_response": s3_response,
            "dynamo_response": resource_response
        }
    }

if __name__ == "__main__":

    lambda_handler({},{})