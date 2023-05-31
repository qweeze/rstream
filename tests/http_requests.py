import requests


def create_exchange(exchange_name: str) -> int:
    request = "http://guest:guest@localhost:15672/api/exchanges/%2f" + "/" + exchange_name
    response = requests.put(request)
    return response.status_code


def delete_exchange(exchange_name: str) -> int:
    request = "http://guest:guest@localhost:15672/api/exchanges/%2f" + "/" + exchange_name
    response = requests.delete(request)
    return response.status_code


def create_binding(exchange_name: str, routing_key: str, stream_name: str):

    data = {
        "routing_key": routing_key,
    }
    request = "http://guest:guest@localhost:15672/api/bindings/%2f/e/" + exchange_name + "/q/" + stream_name

    response = requests.post(request, json=data)
    return response.status_code
