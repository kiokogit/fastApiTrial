from typing import Any, TypeAlias

import requests
from requests.adapters import HTTPAdapter, Retry
from requests.auth import AuthBase

Username: TypeAlias = str
ApiKey: TypeAlias = str
SimpleAuth: TypeAlias = tuple[Username, ApiKey]


class HeaderAuth(AuthBase):
    """Attaches HTTP header auth to the given Request object"""
    def __init__(self, header_key: str, header_val: str):
        self.header_key = header_key
        self.header_val = header_val

    def __call__(self, r):
        # set header auth and return request
        r.headers[self.header_key] = self.header_val
        return r


class ApiSession(requests.Session):
    """A session for making requests to the base_url"""

    def __init__(self, base_url: str, timeout: int, auth: SimpleAuth | AuthBase | None) -> None:
        super().__init__()
        self.base_url = base_url
        self.timeout = timeout
        if auth is not None:
            self.auth = auth
        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
        self.mount('http://', adapter=adapter)
        self.mount('https://', adapter=adapter)

    def request(self, method: str, endpoint: str, *args: Any, **kwargs: Any) -> requests.Response:
        url = self.base_url.format(endpoint=endpoint)
        r = super().request(method, url, *args, timeout=self.timeout, **kwargs)
        r.raise_for_status()
        return r
