import copy
from typing import Union
from typing import Dict, List
import json
import jsonschema
import jsonpatch
import enum
from datetime import datetime
import uuid
import os
import hashlib

from .exceptions import ValidationError, InvalidOperationError

class Model(dict):
    """

    """
    def __init__(self, *args, **kwargs):
        
        in_dict = dict(*args, **kwargs)

        try:
            self.validate(in_dict)
        except Exception as exception:
            raise ValueError(str(exception))
        else:
            dict.__init__(self, in_dict)

        self.__dict__["change_log"] = {}
        self.__dict__["__original__"] = copy.deepcopy(in_dict)

    # Override dict methods

    def __getversion__(self, version:str)->dict:
        return self.__dict__.get("change_log").get(version)
    
    def __diff_version__(self, version:dict)->set:
        """
        Creates a map of the diffs between versions of the object.
        """
        print(set(dict(self).items()))
        print(set(version.items()))
        return set(dict(self).items()) ^ set(version.items())

    def __setversion_metadata__(self, version:dict)-> dict:
        
        return {
            hashlib.md5(json.dumps(dict(self)).encode('utf-8')).hexdigest(): {
            "version_timestamp": str(datetime.now()),
            "version_author": os.getlogin(),
            "changes": self.__diff_version__(version=version),
            "backup": copy.deepcopy(dict(self)) }
        }

    def __setitem__(self, key:str, value:any)->None:
        change_data = dict(self.items())
        change_data[key] = value

        try:
            self.validate(change_data)
        except ValidationError as exception:
            raise InvalidOperationError(
                f"Unable to set {key} due to validation error. Error: {exception}"
            )
        
        # Sets the change log to reflect the previous state of the
        self.__dict__["change_log"].update(self.__setversion_metadata__(change_data))
        dict.__setitem__(self, key, value)

    def __getitem__(self, key:str, version:str=None)->dict:
        
        if version:
            return self.__getversion__(version).get("key")
        
        try:
            return self.__dict__.get("key",None)
        except KeyError as ke:
            raise AttributeError(f"Unable to retrieve {key}. Error: {str(ke)}")
    
    def __getattr__(self, key):
        """
        Returns the value of a given field using dot notation.
        
        Usage:
            from schema_builder import Model

            schema = {"myField":{"type":"string","required":true}}
            model = Model(schema)
            data = {"myField":"Hello!"}
            myModel = model(**data)

            print(myModel.myField)


        """
        try:
            return self.__getitem__(key)
        except KeyError as exception:
            raise AttributeError(exception)
        
    def __setattr__(self, key:str, value:any)->None:
        self.__setitem__(key=key, value=value)

    def __delattr__(self, key):
        self.__delitem__(key)

    def clear(self):
        raise InvalidOperationError()
    
    def pop(self, key, default=None):
        raise InvalidOperationError()
    
    def popitem(self):
        raise InvalidOperationError()
    
    def copy(self)->dict:
        return copy.deepcopy(dict(self))

    def __deepcopy__(self, memo):
        return copy.deepcopy(dict(self),memo)

    def update(self, other):
        change_data = dict(self.items())
        change_data.update(other)

        try:
            self.validate(change_data)
        except ValidationError as exception:
            raise InvalidOperationError(str(exception))
    
        self.__dict__.get("change_log").update(self.__setversion_metadata__(change_data))
        dict.update(self, other)

    def items(self):
        return dict(self).items()
    
    def values(self):
        return dict(self).values()
    
    # Non-standard dict methods
    def ref(self):
        return dict(self)
    
    def clone(self):
        return {
            "instance": copy.deepcopy(self),
            "original": copy.deepcopy(self.__dict__.get("__original__")),
            "change_log": copy.deepcopy(self.__dict__.get("change_log"))
        }

    @property
    def patch(self):
        original = self.__dict__["__original__"]
        return jsonpatch.make_patch(original, dict(self)).to_string()

    def validate(self, object):
        try:
            # This uses the validator instance that is a part of the initial object
            self.validator_instance.validate(object)
        except jsonschema.ValidationError as exception:
            raise ValidationError(str(exception))