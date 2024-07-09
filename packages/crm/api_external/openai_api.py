import json
import time
import re
from json import JSONDecodeError
from typing import Callable
from pprint import pformat

import openai
from loguru import logger
from openai.error import RateLimitError, ServiceUnavailableError, Timeout, APIError

from analysis import AnnotationError

openai.api_key = "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"


class ResponseValidationError(Exception):
    pass


def parse_json_response(project, response_json):
    try:
        res = json.loads(response_json)
    except JSONDecodeError as e:
        logger.error(f'failed parsing chatGPT-generated JSON for {project}: {response_json}')
        raise ResponseValidationError(e)
    return res


def parse_bool_response(response):
    response = response.lower().strip()
    response = re.sub("^\.+", '', response)
    response = re.sub("\.+$", '', response)
    if response == 'true':
        return True
    elif response == 'false':
        return False
    raise ResponseValidationError(f'invalid boolean response: "{response}".'
                                   f' expected "true" or "false")')


async def chat_completion(user_prompt: str,
                          retries: int = 3,
                          response_validator: Callable | None = None,
                          model: str = "gpt-3.5-turbo",
                          include_usage: bool = False):
    ALLOWED_MDELS = [
        "gpt-3.5-turbo",
        "gpt-4",
        "gpt-4-1106-preview"
    ]

    if not model in ALLOWED_MDELS:
        raise ValueError(f"invalid model: {model}")

    if not retries > 0:
        raise ValueError(f"invalid number of retries: {retries}")

    for i in range(0, retries):
        try:
            completion = await openai.ChatCompletion.acreate(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0,
            )

            response = completion.choices[0].message['content']

            if response_validator:
                response = response_validator(response)

            return response if not include_usage else (response, completion.usage)
        except (ResponseValidationError, Timeout, ServiceUnavailableError, APIError) as e:
            logger.error(f'error encountered: {e}')

            match e:
                case ResponseValidationError():
                    logger.error(f'prompt: {pformat(user_prompt)}')

            if i == retries:
                raise AnnotationError('retries limit exceeded for completing '
                                      'chatGPT prompt, unable to annotate')

            logger.info(f'sleeping for {i*30}s.')
            time.sleep(i * 30)
