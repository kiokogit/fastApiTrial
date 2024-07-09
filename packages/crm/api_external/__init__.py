import asyncio
import json
import time

from loguru import logger

import requests
import aiohttp

from requests import HTTPError


class ApiError(Exception):
    def __init__(self, msg, endpoint, payload, response_status, response_text):
        super().__init__(msg)

        self.msg = msg

        self.endpoint: dict = endpoint
        self.payload: dict = payload

        self.response_status: int = response_status
        self.response_text: str = response_text

    def to_dict(self):
        return {
            'msg': self.msg,

            'endpoint': self.endpoint,
            'payload': self.payload,

            'response_status': self.response_status,
            'response_text': self.response_text,
        }


async def async_api_call(method: str, url: str, headers: dict, params: dict, data: dict):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.request(method=method,
                                       url=url,
                                       headers=headers,
                                       params=params,
                                       data=json.dumps(data)) as r:
                # print(f'r url: {r.url} (status{r.status})')
                r.raise_for_status()

                return await r.json()


        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise ApiError(msg=f"Async HTTPError occurred: {str(e)}",
                        endpoint=url,
                        payload=json.dumps({'params': params, 'data': data}),
                        response_status=None,
                        response_text=None)
        except HTTPError as e:
            raise ApiError(msg=f"HTTPError occurred: {str(e)}",
                        endpoint=url,
                        payload=json.dumps({'params': params, 'data': data}),
                        response_status=e.response.status_code,
                        response_text=e.response.text)


def api_call(method: str, url: str, headers: dict, params: dict, data: dict, retries=3):
    if retries < 1:
        raise ValueError("retries must be >= 1")

    i = 0
    while i < retries:
        try:
            r = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                data=json.dumps(data),
            )

            r.raise_for_status()

            return r
        except HTTPError as e:
            i += 1
            if e.errno in (503,):
                time.sleep(30 * i)
                continue
            else:
                break

    r.raise_for_status()
