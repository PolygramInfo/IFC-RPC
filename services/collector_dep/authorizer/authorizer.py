import os
import logging
from datetime import (
    datetime,
    timedelta
)
from dataclasses import dataclass, field
from typing import Dict

import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3 import exceptions

def create_logger()->logging.Logger:
    # Set logging for this module
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    logger.addHandler(stream_handler)

    try:
        session = boto3.Session()
        cloudwatch_logs = session.client("logs")

        cloudwatch_handler = logging.StreamHandler()
        cloudwatch_handler.setLevel(logging.INFO)
        cloudwatch_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        cloudwatch_handler.setStream(
            cloudwatch_logs.create_log_stream(
                logGroupName="collector",
                logStreamName="collector_stream"
            )["logStreamName"]
        )

        logger.addHandler(cloudwatch_handler)
    except exceptions.NoCredentialsError:
        logger.warning("No AWS Credentials found. Logging to CloudWatch will not be available.")   

    return logger

logger = create_logger()

def authorize_user(user_hash:str=None, token:str=None)->int:
    """
    Authorizes a user to access the system.
    """

    if not user_hash or not token:
        logger.info("User hash or token not provided.")
        return 500
    
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("Users") #TODO: Create this table in DynamoDB

    response = table.query(
        KeyConditionExpression=Key("user_hash").eq(user_hash)
    )

    response.order_by("timestamp", ascending=False)

    if response.get("Count") == 0:
        logger.info(f"User {user_hash} not found.")
        return 404

    user = response.get("Items")[0]

    if user.get("token") != token:
        logger.info(f"User {user_hash} provided an invalid token.")
        return 401
    
    if user.get("token_expiration") < datetime.now():
        logger.info(f"User {user_hash} provided an expired token.")
        return 401
    
    return 200

