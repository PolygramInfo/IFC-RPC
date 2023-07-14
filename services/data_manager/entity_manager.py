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

        response = self.dynamodb.get_item(
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

        response["components"] = components
        
        return response
        

    def delete(self, entity_id:str)->dict:
        response = self.dynamodb.delete_item(
            TableName="EntityRegistry",
            Key={
                "entity_id": entity_id
            })
        
        return response

    def register_component(
            self, 
            entity_id:str, 
            component_id:str, 
            component_data:dict
        )->dict:

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

    def relate_entities(
            self, 
            subject:str, 
            object:str,
            relation:str, 
            component_id)->dict:
        
        response = self.dynamodb.update_item(
            TableName="ComponentRegistry",
            Key={
                "component_id": component_id
            },
            UpdateExpression=f"""SET #entities = list_append(#entities, :entity_id),
                            #component_type = :component_type, 
                            #relationship = :relation""",
            ExpressionAttributeNames={
                "#entities": "entities",
                "#component_type": "component_type",
                "#relationship": relation
            },
            ExpressionAttributeValues={
                ":entity_id": {"SS": [subject]},
                ":component_type": {"S": "relationship"},
                ":relation": {"M": {component_id: {"SS": [object]}}}
            })
        
        return response


class ComponentManager:

    def __init__(self)-> None:
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
        
    def create(
            self,
            component_id:str,
            component_type:str,
            component_data:dict
        )->dict:
        
        event = from_dict(CloudEvent, event)

        id = event.get_data()["id"]
        data = event.get_data()["data"]
        data.pop("id")

        response = self.dynamodb.put_item(
            TableName="ComponentRegistry",
            Item={
                "component_id": {"S": event.get_data()["id"]},
                "entities": {"SS": []},
                "component_type": {"S": event.get_attributes()["dataschema"]},
                "component_data": {"M": event["component_data"]}
            },
            ConditionExpression="attribute_not_exists(component_id)",
            ReturnValues="ALL_OLD"
        )

        return response
    
    def read(self, component_id:str)->dict:

        component = self.dynamodb.get_item(
            TableName="ComponentRegistry",
            Key={
                "component_id": {"S": component_id}
            })["Item"]

        return component

def lambda_hander(event, context):
    """
    This function is the entry point for the lambda function.
    """
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    session = boto3.Session()
    s3 = session.client("s3")
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table("Resources")

    event = from_dict(
        event_type=CloudEvent,
        event=json.loads(json.loads(event)["Body"])
    )

    entity_manager = EntityManager()
    component_manager = ComponentManager()

    action = event["type"].split("/")[-1]
    if action == "entity.create":
        response = entity_manager.create(event.get_data()["primitive_type"])
        
        if event.get_data().get("components", None):
            for component, component_name in event.get_data()["components"].items():
                component_id = component.get("id", uuid.uuid4().hex)
                try:
                    component_manager.create(
                        component_id=component_id,
                        component_type=component_name,
                        component_data=component["data"]
                    )
                except KeyError as err:
                    logger.error(f"Unable to create component: {err}")
                    raise err
                
                entity_manager.register_component(
                    entity_id=response["Attributes"]["entity_id"],
                    component_id=component_id
                )
                    
    elif action == "entity.get":
        entity_manager.read(event["entity_id"], event.get_data().get("filter", None))
    elif action == "entity.register":
        if len(event.get("data",{}).get("components", {})) > 0:
            for component in event.get_data()["components"]:
                entity_manager.register_component(
                    entity_id=event.get_data()["entity_id"],
                    component_id=component)
        else:
            entity_manager.register_component(
                entity_id=event.get_data()["entity_id"],
                component_id=event.get_data()["component_id"])
    elif action == "entity.relate":
        response = entity_manager.relate_entities(
            event["subject"], 
            event["object"],
            event["relation_type"], 
            event["component_id"])
    elif action == "entity.delete":
        entity_manager.delete(event["entity_id"])
    elif action == "component.create":
        response = component_manager.create(
            component_id=event.get_data()["id"],
            component_type=event.get_attributes()["dataschema"],
            component_data=event.get_data()["data"]
        )
    else:
        raise Exception(f"Invalid event type: {event['type']}")

    bucket = "pg-resource-registery"
    key = f"resources/{event['resource_id']}.json"

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

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    handler = logging.FileHandler("EntityManager.test.log")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)

    logger.addHandler(handler)

    sqs = boto3.client("sqs")
    queue_url = sqs.get_queue_by_name(QueueName="EntityManagerQueue")

    message = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20
    )["Messages"][0]

    response = lambda_hander(json.dumps(message), None)
    print(response)