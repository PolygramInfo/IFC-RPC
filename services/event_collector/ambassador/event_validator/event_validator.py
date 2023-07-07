import os
import logging
import sys

import json
import jsonschema

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
logger.addHandler(handler)

def validate_event(event:dict)->bool:
    """
    Validate the event conforms to event schema this step
    does not check the data of the event, only the event is
    a valid event type and has the required fields.
    """

    with open(os.path.join(os.path.dirname(__file__), "event_schema.json"), "r") \
          as schema_file:
        schema = json.load(schema_file)

    try:
        jsonschema.validate(event, schema)
        return True
    except jsonschema.exceptions.ValidationError as err:
        logging.err
        return False