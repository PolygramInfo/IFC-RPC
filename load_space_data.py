from sdk.request import Event
#from sdk import schema_builder
from services.event_collector import (
    Ambassador,
    Validator,
    Router
)

from sdk.designSchema import design_wall
import logging
import hashlib, sys, json

import boto3
from cloudevents.conversion import to_dict
import uuid
import time

from services.data_manager import schema_manager, entity_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
stream_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)

logger.info("Starting data load for energy space registration")

with open("out_entity_broken.json","r") as f:
    entity = json.load(f)

event = Event()\
    .set_attributes(
        service="entity",
        request_type="create",
        userhash=hashlib.sha256("zach@polygram.info".encode()).hexdigest(),
        transasction_id=uuid.uuid4().hex,
        usertoken="8048a8c0aea04628b2f2ff4f9cb92fd7.96d3a924b3814d2f9b4d7db388c44585"
    )\
    .set_schema(
        domain="meta",
        schema_name="Entity"
    )\
    .set_data(
        **entity
    ).serialize()

logger.info(f"Event created {event.get_attributes()['id']}")
logger.debug(f"Event created.\n {json.dumps(to_dict(event), indent=4)}")

reply = Ambassador().main(to_dict(event))
resource_id = json.loads(reply["body"])["data"]["resource_id"]
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