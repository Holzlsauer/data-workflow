from datetime import date, datetime
from pydantic import BaseModel

class User(BaseModel):
    name: str
    title: str
    email: str
    gender: str
    nat: str
    cellphone: str
    picture: str
    birthday: date
    registered: datetime