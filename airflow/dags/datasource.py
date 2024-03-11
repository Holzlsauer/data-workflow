import json
import logging
import requests
import random
from datetime import datetime
from models.user import User
from utils import KafkaProducer
from typing import Dict, List

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)


def extract_user_information(data: Dict[str, str]) -> User:
    """
    Extracts user information from the provided data.

    Args:
        data (dict): Raw user data.

    Returns:
        User: User object containing user information.
    """
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ" 
    return User(
        name=f"{data['name']['first']} {data['name']['last']}",  # type: ignore
        title=data['name']['title'],  # type: ignore
        email=data['email'],
        gender=data['gender'],
        nat=data['nat'],
        cellphone=data['cell'],
        picture=data['picture']['large'],  # type: ignore
        birthday=datetime.strptime(data['dob']['date'], date_format).strftime('%Y-%m-%d'),  # type: ignore
        registered=float(datetime.strptime(data['registered']['date'], date_format).timestamp() * 1000) # type: ignore
    )


def fetch_data() -> List[User]:
    """
    Fetches user data from the randomuser.me API.

    Raises:
        Exception: If the request does not return a status code of 200.

    Returns:
        list[User]: List of User objects containing user information.
    """
    response = requests.get(
        f'https://randomuser.me/api/?results={random.randint(10, 20)}&nat=BR'
    )

    if not response.status_code == 200:
        raise Exception(f'Error when fetching data: {response.text}')
    
    # meta_data = response.json().get('info')
    user_data = response.json().get('results')

    data = [extract_user_information(user) for user in user_data]
    return data

def send_data() -> None:
    """
    Fetches user data and sends it to a Kafka topic.

    Uses KafkaProducer to produce messages for each user's data.

    Returns:
        None
    """
    logging.info('----- Start DAG -----')
    users = fetch_data()
    streaming_data = [user.__dict__ for user in users]
    producer = KafkaProducer()
    for data in streaming_data:
        print(type(data))
        producer.produce(str(data))

if __name__ == '__main__':
    send_data()
