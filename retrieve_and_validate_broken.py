from sdk.request import Event
#from sdk import schema_builder
from services.event_collector import (
    Ambassador,
    Validator,
    Router
)

from sdk.designSchema import design_wall
from sdk.energySchema import energy_space

import logging
import hashlib, sys, json

import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer
from cloudevents.conversion import to_dict
import uuid
import time

import jsonschema

from services.data_manager import schema_manager, entity_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
stream_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("retrieve_and_validate.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

"""
logger.info("Starting getting component")

event = Event()\
    .set_attributes(
        service="entity",
        request_type="componentRead",
        userhash=hashlib.sha256("zach@polygram.info".encode()).hexdigest(),
        transasction_id=uuid.uuid4().hex,
        usertoken="8048a8c0aea04628b2f2ff4f9cb92fd7.96d3a924b3814d2f9b4d7db388c44585"
    )\
    .set_schema(
        domain="command",
        schema_name="Read"
    )\
    .set_data(
        id = "c34d6b571696419dbb052e553661211a"
    ).serialize()

logger.info(f"Event created {event.get_attributes()['id']}")
logger.debug(f"Event created.\n {json.dumps(to_dict(event), indent=4)}")

reply = Ambassador().main(to_dict(event))
resource_id = json.loads(reply["body"])["data"]["resource_id"]
logger.info(f"Resource ID: {resource_id}")
if reply["statusCode"] > 299:
    logger.error("Authentication failed!")
    raise ValueError("Authentication failed.")
time.sleep(15)
logger.debug(f"Reply received from Ambassador.\n {json.dumps(reply, indent=4)}")
reply = Validator().main()
time.sleep(15)
logger.debug(f"Reply received from Validator.\n {json.dumps(reply, indent=4)}")
reply = Router().main()
time.sleep(15)
logger.debug(f"Reply received from Router.\n {json.dumps(reply, indent=4)}")
time.sleep(15)
logger.debug(f"Firing entity manager")
reply = entity_manager.lambda_hander({}, {})
logger.debug(f"Reply received from entity manager.\n {json.dumps(reply, indent=4)}")

time.sleep(30)
"""

resource_id = "af2c058024d043608f5f083b14ac0ce1"
logger.info(f"Getting resource {resource_id}")

sessions = boto3.Session()
dynamodb = sessions.client('dynamodb')
s3 = sessions.client('s3')

response = dynamodb.query(
    TableName="Resources",
    KeyConditionExpression="resource_id = :resource_id",
    FilterExpression="published = :attrValue",
    ExpressionAttributeValues={
        ":resource_id": {"S": resource_id},
        ":attrValue": {"BOOL": True}
    })["Items"][0]

ds = TypeDeserializer()

item = {k: ds.deserialize(v) for k, v in response.items()}

logger.debug(f"Item received from DynamoDB.\n {json.dumps(item, indent=4)}")

bucket = "pg-resource-registery"
key = f"resources/{resource_id}.json"
component = json.loads(s3.get_object(
    Bucket=bucket,
    Key=key)["Body"].read().decode("utf-8"))

logger.debug(f"Component received from S3.\n {json.dumps(component, indent=4)}")

schema = energy_space().__dict__["schema"]

try: 
    logger.info(f"Validating component {component['component_id']}")
    logger.debug(f"Schema used for validation.\n {json.dumps(component['component_data'], indent=4)}")
    jsonschema.validate(component["component_data"], schema)
except jsonschema.exceptions.ValidationError as e:
    logger.error(f"Validation failed. {e}")
    event = Event()\
        .set_attributes(
        service="error",
        request_type="invalidData",
        userhash=hashlib.sha256("zach@polygram.info".encode()).hexdigest(),
        transasction_id=uuid.uuid4().hex,
        usertoken="8048a8c0aea04628b2f2ff4f9cb92fd7.96d3a924b3814d2f9b4d7db388c44585"
    )\
    .set_schema(
        domain="system",
        schema_name="Error"
    )\
    .set_data(
        message="Validation failed for received.",
        error=str(e)
    ).serialize()

    logger.error(f"Validation failed.\n {e}")
    logger.debug(f"Error event created.\n {json.dumps(to_dict(event), indent=4)}")
    raise ValueError("Validation failed.")