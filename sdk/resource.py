import time
import threading

from .request import Event
import boto3

class ResourceListener:
    __next_try__: int
    __func__: any
    _args: list
    _kwargs: dict
    __is_running__: bool=False
    __resource_table__ = "Resources"

    def __init__(self, resource_id:str, function:str, *args, **kwargs) -> None:
        self.__resource_id__ = resource_id
        self.__func__ = function
        self._args = args
        self._kwargs = kwargs

        if kwargs.get("auto_start", False):
            self.start()

    def __run__(self)->None:
        self.__is_running__ = False
        self.start()
        response = self.__func__(*self._args, **self._kwargs)
        self.stop()

        return response

    def start(self)->None:
        if not self.__is_running__:
            while self.__next_try__ >= time.time():
                response = self.get_resource(self.__resource_id__)
                if response.get("Item").get("published",False):
                    
                    

    def stop(self)->None:
        self.__is_running__ = False

    def get_resource(resource_id:str)->dict:
        
        dynamodb = boto3.client("dynamodb")
        response = dynamodb.get_item(
            TableName="Resources",
            Key={
                "resource_id": {"S": resource_id}
            }
        )

        return response["Item"]