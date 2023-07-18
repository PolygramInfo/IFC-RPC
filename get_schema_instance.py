from sdk.request import Event
#from sdk import schema_builder
from services.event_collector import (
    Ambassador,
    Validator,
    Router,
)

from sdk.designSchema import design_wall
import logging
import hashlib, sys, json

import boto3
from cloudevents.conversion import to_dict
import uuid
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
stream_handler.setLevel(logging.DEBUG)
with open("schema_registration.log","w") as f:
    f.truncate(0)

file_handler = logging.FileHandler("schema_read.log")

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

logger.info("Starting schema registration")
schema = design_wall()
logger.info("Schema built")
logger.debug(f"Schema definitions.\n {json.dumps(schema.__dict__['schema'])}")

logger.info("Creating event")
event = Event()\
    .set_attributes(
        service="schema",
        request_type="read",
        userhash=hashlib.sha256("zach@polygram.info".encode()).hexdigest(),
        transasction_id=uuid.uuid4().hex,
        usertoken="8048a8c0aea04628b2f2ff4f9cb92fd7.96d3a924b3814d2f9b4d7db388c44585"
    )\
    .set_schema(
        domain="command",
        schema_name="Read"
    )\
    .set_data(
    schema_domain="design",
    schema_name="Wall",
    ).serialize()

logger.debug(f"Event created.\n {json.dumps(to_dict(event), indent=4)}")

logger.info("Sending event to Ambassador")
reply = Ambassador().main(to_dict(event))
time.sleep(1)
if reply["statusCode"] > 299:
    logger.error("Authentication failed!")
    raise ValueError("Authentication failed.")
logger.debug(f"Reply received from Ambassador.\n {json.dumps(reply, indent=4)}")
reply = Validator().main()
time.sleep(1)
logger.debug(f"Reply received from Validator.\n {json.dumps(reply, indent=4)}")
reply = Router().main()
time.sleep(1)
logger.debug(f"Reply received from Router.\n {json.dumps(reply, indent=4)}")