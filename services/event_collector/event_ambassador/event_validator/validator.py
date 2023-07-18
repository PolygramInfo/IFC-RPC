import os
import logging
import sys

import json
import jsonschema

from cloudevents.conversion import to_json

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
logger.addHandler(handler)

def validate_event(event:dict)->bool:
    """
    Validate the event conforms to event schema this step
    does not check the data of the event, only the event is
    a valid event type and has the required fields.
    """

    logger.info("Validating event")
    logger.debug(f"Event Type: {type(event)}")
    logger.debug(f"Event: {event}")

    with open(os.path.join(os.path.dirname(__file__), "event.schema.json"), "r") \
          as schema_file:
        schema = json.load(schema_file)

    try:
        jsonschema.validate(event, schema)
        return True
    except jsonschema.exceptions.ValidationError as err:
        logger.error(f"Unable to validate event: {err.message}")
        logger.info(f"Event error: {err}")
        return False