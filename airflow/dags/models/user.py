from pydantic import BaseModel

class User(BaseModel):
    name: str
    title: str
    email: str
    gender: str
    nat: str
    cellphone: str
    picture: str
    birthday: str
    registered: float