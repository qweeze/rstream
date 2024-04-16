import urllib.parse

import requests
from requests.auth import HTTPBasicAuth


def get_connections() -> list:
    request = "http://localhost:15672/api/connections"
    response = requests.get(request, auth=HTTPBasicAuth("guest", "guest"))
    response.raise_for_status()
    return response.json()


def get_connection(name: str) -> bool:
    request = "http://guest:guest@localhost:15672/api/connections/" + urllib.parse.quote(name)
    response = requests.get(request, auth=HTTPBasicAuth("guest", "guest"))
    if response.status_code == 404:
        return False
    return True


def get_connection_present(connection_name: str, connections: list) -> bool:
    for connection in connections:
        if connection["client_properties"]["connection_name"] == connection_name:
            return True
    return False


def delete_connection(name: str) -> int:
    request = "http://guest:guest@localhost:15672/api/connections/" + urllib.parse.quote(name)
    response = requests.delete(request, auth=HTTPBasicAuth("guest", "guest"))
    return response.status_code
