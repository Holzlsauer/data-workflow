import requests
import random

from datetime import datetime

from models.user import User


def extract_user_information(data: dict[str, str]) -> User:
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
        birthday=datetime.strptime(data['dob']['date'], date_format).date(),  # type: ignore
        registered=datetime.strptime(data['registered']['date'], date_format)  # type: ignore
    )


def fetch_data() -> list[User]:
    """
    Fetches user data from the randomuser.me API.

    Raises:
        Exception: If the request does not return a status code of 200.

    Returns:
        list[User]: List of User objects containing user information.
    """
    response = requests.get(
        f'https://randomuser.me/api/?results={random.randint(1000, 5000)}&nat=BR'
    )

    if not response.status_code == 200:
        raise Exception(f'Error when fetching data: {response.text}')
    
    # meta_data = response.json().get('info')
    user_data = response.json().get('results')

    data = [extract_user_information(user) for user in user_data]
    return data

if __name__ == '__main__':
    users = fetch_data()
    streaming_data = [user.__dict__ for user in users]
