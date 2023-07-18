import logging
import sys
import uuid
from dataclasses import dataclass
import json
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
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

    def create(self, id:str, primitive_type:str=None)->None:

        response = self.dynamodb.put_item(
            TableName="EntityRegistry",
            Item={
                "primitive_type": {"S": primitive_type if primitive_type else ""},
                "entity_id": {"S":id}
            },
            ReturnValues="ALL_OLD"
        )

        return response

    def read(self, entity_id:str, filter:str=None)->dict:
        """
        Return an entity and all of its components.
        """
        self.logger.debug(f"Running read for {entity_id}")

        ds = TypeDeserializer()
        response = self.dynamodb.get_item(
            TableName="EntityRegistry",
            Key={
                "primitive_type": {"S":"wall"},
                "entity_id": {"S": entity_id}
            })["Item"]
        self.logger.debug(f"Got Item:\n {response}")

        response = {k: ds.deserialize(v) for k,v in response.items()}
        
        self.logger.debug(f"Deserialized Object:\n {response}")

        temp = self.dynamodb.scan(
            TableName="ComponentRegistry",
            FilterExpression="contains(#entities, :entity_id)",
            ExpressionAttributeNames={
                "#entities": "entities"
            },
            ExpressionAttributeValues={
                ":entity_id": {"S": entity_id}
            })["Items"]
        
        self.logger.debug(f"Component Response: {temp}")

        components = []
        for component in temp:
            self.logger.debug(f"Deserializing {component}")
            component = {k: ds.deserialize(v) for k,v in component.items()}
            self.logger.debug(f"Deserialized Component:\n {component}")
            components.append(component)

        if filter:
            components = [component for component in components if component["component_type"].split(".")[0] == filter]

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
            component_type:str
        )->dict:

        self.dynamodb.update_item(
            TableName="ComponentRegistry",
            Key={
                "component_id": {"S": component_id},
                "component_type": {"S": component_type}
            },
            UpdateExpression="SET #entities=list_append(#entities, :entity_id)",
            ExpressionAttributeNames={
                "#entities": "entities"
            },
            ExpressionAttributeValues={
                ":entity_id": {"L":[{"S": entity_id}]}
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
            component_data:dict,
            entity_id:str=None
        )->dict:

        ts = TypeSerializer()
        Item = {
            "component_id": component_id,
            "component_type": component_type,
            "entities":[entity_id] if entity_id else [],
            "component_data": component_data
        }

        response = self.dynamodb.put_item(
            TableName="ComponentRegistry",
            Item=ts.serialize(Item)["M"],
            ReturnValues="ALL_OLD"
        )

        return response
    
    def read(self, component_id:str)->dict:

        ds = TypeDeserializer()

        component = self.dynamodb.query(
            TableName="ComponentRegistry",
            KeyConditionExpression="component_id = :component_id",
            ExpressionAttributeValues={
                ":component_id": {"S": component_id}
            })["Items"][0]
        
        return {k: ds.deserialize(v) for k,v in component.items()}
    

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def lambda_hander(event, context):
    """
    This function is the entry point for the lambda function.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    #boto3.set_stream_logger('', level=logging.DEBUG)
    session = boto3.Session()
    sqs = session.resource("sqs")
    s3 = session.client("s3")
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table("Resources")
    
    queue = sqs.get_queue_by_name(QueueName="data-queue")
    raw_event = queue.receive_messages(
        MaxNumberOfMessages=1
    )

    if not len(raw_event):
        logger.info("No messages in queue.")
        return {"message":"No messages in queue."}

    logger.debug(f"Raw Event: {raw_event[0].body}")

    receipt_handle = raw_event[0].receipt_handle
    event = from_dict(
        event_type=CloudEvent,
        event=json.loads(raw_event[0].body, parse_float=Decimal)
    )

    entity_manager = EntityManager()
    component_manager = ComponentManager()

    action = event["type"]
    if action == "com.pg.entity/create":
        response = entity_manager.create(event.get_data()["id"], event.get_data()["primitive_type"])
        
        if event.get_data().get("components", None):
            for component_name, component in event.get_data()["components"].items():
                logger.debug(f"Loading Component:\n {component}")
                component_id = component.get("id", uuid.uuid4().hex)
                try:
                    component_response = component_manager.create(
                        component_id=component_id,
                        component_type=component_name,
                        component_data=component,
                        entity_id=event.get_data().get("id",None)
                    )
                except KeyError as err:
                    logger.error(f"Unable to create component: {err}")
                    raise err
                
                entity_manager.register_component(
                    entity_id=event.get_data()["id"],
                    component_id=component_id,
                    component_type=component_name
                )

                response.update({component_name:component_response})
    elif action == "com.pg.entity/read":
        response = entity_manager.read(event.get_data()["id"], event.get_data().get("filter", None))
    elif action == "com.pg.entity/registerComponent":
        if len(event.get("data",{}).get("components", {})) > 0:
            for component in event.get_data()["components"]:
                entity_manager.register_component(
                    entity_id=event.get_data()["entity_id"],
                    component_id=component)
        else:
            entity_manager.register_component(
                entity_id=event.get_data()["entity_id"],
                component_id=event.get_data()["component_id"])
    elif action == "com.pg.entity/relate":
        response = entity_manager.relate_entities(
            event["subject"], 
            event["object"],
            event["relation_type"], 
            event["component_id"])
    elif action == "entity.delete":
        entity_manager.delete(event["entity_id"])
    elif action == "com.pg.entity/componentCreate":
        response = component_manager.create(
            component_id=event.get_data()["id"],
            component_type=event.get_attributes()["dataschema"],
            component_data=event.get_data()["data"]
        )
    elif action == "com.pg.entity/componentRead":
        response = component_manager.read(event.get_data()["id"])
    else:
        raise Exception(f"Invalid event type: {event['type']}")

    bucket = "pg-resource-registery"
    key = f"resources/{event['resource_id']}.json"

    self.logger.debug(f"Updating resource: {event.get_attributes()['resource_id']}")

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

    queue.delete_messages(
        Entries=[
            {"Id": raw_event[0].message_id, "ReceiptHandle": raw_event[0].receipt_handle}
        ]
    )

    if resource_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise Exception(f"Unable to update resource: {resource_response}")    

    logger.debug(f"Response:\n {response}")

    s3_put = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(response, indent=4, cls=JSONEncoder)
    )

    logger.info(f"Writing resource {event.get_attributes()['resource_id']} to {bucket}/{key}.json")
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

    lambda_hander({}, {})