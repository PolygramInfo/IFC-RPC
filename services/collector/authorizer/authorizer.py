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

class PolicyDocument:
    
    def __init__(self, user_id):

        self.document = {
            "principalID": str(user_id),
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "execute-api:Invoke",
                        "Effect": effect,
                        "Resource": resource
                    }
                ]
            }
        }

def lambda_handler(event, context):
    
    