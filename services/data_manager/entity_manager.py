import logging
import sys
import uuid
from dataclasses import dataclass
import json

import boto3
import botocore

from cloudevents.http import CloudEvent
from cloudevents.conversion import from_dict, to_dict

class EntityManager:

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

    def create(self, primitive_type:str=None)->None:

        response = self.dynamodb.put_item(
            TableName="EntityRegistry",
            Item={
                "primitive_type": {"S": primitive_type if primitive_type else ""},
                "entity_id": {"S":uuid.uuid4().hex}
            },
            ConditionExpression="attribute_not_exists(entity_id)",
            ReturnValues="ALL_OLD"
        )

        return response

    def read(self, entity_id:dict, filter:str=None)->dict:
        """
        Return an entity and all of its components.
        """

        entity = self.dynamodb.get_item(
            TableName="EntityRegistry",
            Key={
                "entity_id": entity_id
            })["Item"]
        
        components = self.dynamodb.scan(
            TableName="ComponentRegistry",
            FilterExpression="contains(#entities, :entity_id)",
            ExpressionAttributeNames={
                "#entities": "entities"
            },
            ExpressionAttributeValues={
                ":entity_id": entity_id
            })["Items"]
        
        if filter:
            components = [component for component in components if component["component_type"] == filter]

        entity["components"] = components
        
        return entity
        

    def delete(self, entity:dict)->None:
        response = self.dynamodb.delete_item(
            TableName="EntityRegistry",
            Key={
                "entity_id": entity["entity_id"]
            })
        
        return response

    def register_component(self, entity_id:str, component_id:str)->None:
        
        self.dynamodb.update_item(
            TableName="ComponentRegistry",
            Key={
                "component_id": entity_id
            },
            UpdateExpression="ADD #entities :entity_id",
            ExpressionAttributeNames={
                "#entities": "entities"
            },
            ExpressionAttributeValues={
                ":entity_id": {"SS": [entity_id]}
            })

    def relate_entities(self, subject:str, object:str, component_id:dict)->None:
        pass

def lambda_hander(event, context):
    """
    This function is the entry point for the lambda function.
    """

    session = boto3.Session()
    s3 = session.client("s3")
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table("Resources")

    event = from_dict(
        event_type=CloudEvent,
        event=json.loads(json.loads(event)["Body"])
    )

    entity_manager = EntityManager()

    if event["type"] == "com.data.entity.create":
        response = entity_manager.create(event.get_data()["primitive_type"])
    elif event["type"] == "com.data.entity.get":
        if event.get("data", {}).get("filter", None):
            entity_manager.read(event["entity_id"], event["data"]["filter"])
        entity_manager.read(event["entity_id"])
    elif event["type"] == "com.data.entity.register":
        if len(event.get("data",{}).get("components", {})) > 0:
            for component in event["data"]["components"]:
                entity_manager.register_component(component)
    elif event["type"] == "com.data.entity.relate":
        entity_manager.relate_entities(
            event["subject"], 
            event["object"],
            event["relation_type"], 
            event["component_id"])

    bucket = "pg-resource-registery"
    key = f"/resources/{event['resource_id']}.json"

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
            ":resource_url": f"https://s3.amazonaws.com/{bucket}/{key}"
        })

    if resource_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise Exception(f"Unable to update resource: {resource_response}")

    

    s3_put = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(response)
    )

    if s3_put["ResponseMetadata"]["HTTPStatusCode"] != 200:
        table.update_item(
            Key={
                "resource_id": event.get_attributes()["resource_id"],
                "updated_at": event.get_attributes()["time"]
            },
            UpdateExpression="SET(#published, :status)",
            ExpressionAttributeNames={
                "#published": "published"
            },
            ExpressionAttributeValues={
                ":published": False
            })
        
        raise Exception(f"Unable to update resource: {s3_put}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "resource_id": event["resource_id"],
            "status": "published"
        })}

if __name__ == "__main__":

    attributes = {
        "resource_id": "1234",
        "type": "com.data.entity.create",
        "source": "com.data.test",
        "datacontenttype": "application/json",
    }

    data = {
        "primitive_type": "test"
    }

    event = CloudEvent(attributes, data)

    message = {
        "MessageId": "1234",
        "ReceiptHandle": "1234",
        "Body": json.dumps(to_dict(event)),
        "Attributes": {},
        "MessageAttributes": {},
        "MD5OfBody": "1234",
        "MD5OfMessageAttributes": "1234",
        "EventSource": "aws:sqs",
    }

    response = lambda_hander(json.dumps(message), None)
    print(response)