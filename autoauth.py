import requests
from utils import get_auth_header, auto_token_refresh


@auto_token_refresh
def get(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.get(*args, **kwargs)
    return r


@auto_token_refresh
def post(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.post(*args, **kwargs)
    return r


@auto_token_refresh
def patch(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.get(*args, **kwargs)
    return r


@auto_token_refresh
def patch(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.get(*args, **kwargs)
    return r
