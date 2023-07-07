
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
import logging
import sys
import json
from http import HTTPStatus

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s"))
logger.addHandler(handler)

def get_user(user_hash:str=None)->dict:
    """
    Get a user from the dynamoDB user table.
    """

    if not user_hash:
        logger.info("User hash not provided.")
        return {"status": False, "message": "User hash not provided."} 
    
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("Users") #TODO: Create this table in DynamoDB

    response = table.query(
        KeyConditionExpression=Key("user_hash").eq(user_hash)
    )

    response.order_by("timestamp", ascending=False)

    if response.get("Count") == 0:
        logger.info(f"User {user_hash} not found.")
        return {"status": False, "message": f"User {user_hash} not found."}

    return {"status": True, "data": response.get("Items")[0]}

def validate_token(user_record:str=None, token:str=None)->dict:
    """
    Validate a token against a user record.
    """

    if not user_record or not token:
        logger.info("User record or token not provided.")
        return {"status": False, "message": "User record or token not provided."}

    if user_record.get("token") != token:
        logger.info(f"User {user_record.get('user_hash')} provided an invalid token.")
        return {"status": False, "message": f"User {user_record.get('user_hash')} provided an invalid token."}
    
    if user_record.get("token_expiration") < datetime.now():
        logger.info(f"User {user_record.get('user_hash')} provided an expired token.")
        return {"status": False, "message": f"User {user_record.get('user_hash')} provided an expired token."}
    
    return {"status": True, "message": "Token is valid."}

def authorize_user(user_hash:str=None, token:str=None)->dict:
    """
    Authorizes a user to access the system.
    """
 
    if not user_hash:
        logger.info("User hash not provided.")
        return {
            "status": False,
            "http_status": HTTPStatus.BAD_REQUEST,
            "message": "Bad request: User hash not provided."
        }
    
    session = boto3.Session()
    dynamodb = session.resource("dynamodb", region_name="us-east-1")
    try:
        response = dynamodb.list_tables()
        if "Users" not in response.get("TableNames"):
            logger.info("User table not found.")
            return {
                "status": False,
                "http_status": HTTPStatus.INTERNAL_SERVER_ERROR,
                "message": "User table not found."
            }
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        logger.error(f"Error listing tables in DynamoDB.")
        logger.error(e)
        return {
            "status": False,
            "http_status": HTTPStatus.INTERNAL_SERVER_ERROR,
            "message": "Server error: Retrying may resolve this issue."
        }

    try:
        user_record = get_user(user_hash=user_hash)
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        """
        If the user table does not exist, this will throw an exception.
        """
        logger.error(f"Error getting user record for {user_hash}.")
        logger.error(e)
        return {
            "status": False,
            "http_status": HTTPStatus.INTERNAL_SERVER_ERROR,
            "message": f"Server error: Unable to retrieve user records. \
                Retrying may resolve this issue."
        }

    if not user_record.get("status") or not user_record.get("data"):
        logger.info(f"Record for {user_hash} not found in user table.")
        logger.debug(f"User record: {json.dumps(user_record, indent=4)}")
        return {
            "status": False,
            "http_status": HTTPStatus.NOT_FOUND,
            "message": f"Record for user not found in user table. Please register first."
        }
    
    if not token:
        logger.info("Token not provided.")
        return {
            "status": False,
            "http_status": HTTPStatus.BAD_REQUEST,
            "message": "Token not provided."
        }

    if not validate_token(user_record=user_record.get("data"), token=token).get("status"):
        logger.info(f"Token validation failed for {user_hash}.")
        logger.debug(f"User record: {json.dumps(user_record, indent=4)}")
        return {
            "status": False,
            "http_status": HTTPStatus.UNAUTHORIZED,
            "message": f"Token validation failed."
        }
    
    return {
        "status": True,
        "http_status": HTTPStatus.OK,
        "message": "User authorized."
    }