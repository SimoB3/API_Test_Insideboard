import datetime
from enum import Enum

from pydantic import BaseModel, EmailStr
import uuid


class SexEnum(str, Enum):
    male = "M"
    female = "F"


class Client(BaseModel):
    id: uuid.UUID
    name: str
    sex: SexEnum
    address: str
    mail: EmailStr
    birthdate: datetime.date


class Item(BaseModel):
    id: uuid.UUID
    name: str
    price: float
