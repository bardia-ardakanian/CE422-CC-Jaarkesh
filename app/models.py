from enum import Enum
from django.db import models
from datetime import datetime
import json


# Create your models here.
class Response:

    def __init__(self, error_code, error_message, body):
        self.error_code = error_code
        self.error_message = error_message
        self.body = body
        self.count = len(body)
        self.datetime = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")

    def json(self):
        return json.dumps(self.__dict__, indent=4)


class State(Enum):
    PROCESSING = 1
    ACCEPTED = 2
    REJECTED = 3
