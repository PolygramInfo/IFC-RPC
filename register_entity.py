from sdk.request import Event
#from sdk import schema_builder
from services.event_collector import (
    Ambassador,
    Validator,
    Router,
)

from sdk.designSchema import design_wall
from sdk.energySchema import energy_wall

import logging
import hashlib, sys, json

from cloudevents.conversion import to_dict
import uuid
import time

from services.data_manager import entity_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
stream_handler.setLevel(logging.DEBUG)
with open("entity_registration.log","w") as f:
    f.truncate(0)

file_handler = logging.FileHandler("entity_registration.log")

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

logger.info("Starting schema registration")
schema = design_wall()
logger.info("Schema built")
logger.debug(f"Schema definitions.\n {json.dumps(schema.__dict__['schema'])}")

logger.info("Creating event")

#Used for initial registration entity_id = "ec596852c68d467c9fd74f349160d48a"

entity_id = "1dd8198ab83d4535b2cde49b877ac7a9"

wall = design_wall().factory("NewWall")
d_instance = wall(
    id = uuid.uuid4().hex,
    describes = "wall",
    subcomponents = {
        "properties":{
            "dimensions": {
                "length":10,
                "height":10,
                "width":0.5
            }
        }
    }
)

e_wall = energy_wall().factory("NewWall")
e_instance = e_wall(
    id = uuid.uuid4().hex,
    describes="wall",
    subcomponents = {
        "properties":{
            "dimensions":{
                "length":10,
                "height":10
            }
        }
    }
)

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
        id = entity_id,
        primitive_type = "wall",
        components = {
            "design.Wall":d_instance.copy(),
            "energy.Wall":e_instance.copy()
        }
    ).serialize()

logger.debug(f"Event created.\n {json.dumps(to_dict(event), indent=4)}")

logger.info("Sending event to Ambassador")
reply = Ambassador().main(to_dict(event))
time.sleep(10)
if reply["statusCode"] > 299:
    logger.error("Authentication failed!")
    raise ValueError("Authentication failed.")
logger.debug(f"Reply received from Ambassador.\n {json.dumps(reply, indent=4)}")
reply = Validator().main()
time.sleep(10)
logger.debug(f"Reply received from Validator.\n {json.dumps(reply, indent=4)}")
reply = Router().main()
time.sleep(10)
logger.debug(f"Reply received from Router.\n {json.dumps(reply, indent=4)}")
time.sleep(10)
logger.debug(f"Firing entity manager")
reply = entity_manager.lambda_hander({}, {})
logger.debug(f"Reply received from schema manager.\n {json.dumps(reply, indent=4)}")