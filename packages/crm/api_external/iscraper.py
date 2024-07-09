from api_external import ApiError, api_call, async_api_call

ISCRAPER_V2_URL = 'https://api.iscraper.io/v2'
ISCRAPER_V3_URL = 'https://api.iscraper.io/v3'
API_KEY = 'dpP3i4oSUMjifQFyBx9VJlBpr4cQMu7F'


def iscraper_request(method: str, url: str, headers: dict, params: dict, data: dict):
    r = api_call(
                 method=method,
                 url=url,
                 headers=headers,
                 params=params,
                 data=data,
         )
    return r


def iscraper_request_v2(method: str, endpoint, payload):
    url = f'{ISCRAPER_V2_URL}/{endpoint}'

    r = iscraper_request(
                 method=method,
                 url=url,
                 headers={
                        'content-type': 'application/json',
                        'X-API-KEY': API_KEY,
                    },
                 params={},
                 data=payload,
                 )
    response = r.json()

    if 'details' in response and not isinstance(response['details'], dict):
        raise ApiError('iScraper has failed',
                       endpoint=url, payload=payload,
                       response_status=r.status_code, response_text=r.text)

    return response


def iscraper_request_v3(method: str, endpoint: str, payload: dict):
    url = f'{ISCRAPER_V3_URL}/{endpoint}'

    auth_payload = payload.copy()
    auth_payload['api_key'] = API_KEY

    r = iscraper_request(
                 method=method,
                 url=url,
                 headers={},
                 params=auth_payload,
                 data={}
                 )
    response = r.json()

    return response


async def iscraper_v3_async(method: str, endpoint: str, payload: dict):
    url = f'{ISCRAPER_V3_URL}/{endpoint}'

    auth_payload = payload.copy()
    auth_payload['api_key'] = API_KEY

    return await async_api_call(method=method,
                                url=url,
                                headers={},
                                params=auth_payload,
                                data={})


async def profile_details_v3_async(**kwargs):
    return await iscraper_v3_async('get', endpoint='people/profile-details', payload=kwargs)


async def profile_company_details_v3_async(**kwargs):
    return await iscraper_v3_async('get', endpoint='companies/profile-details', payload=kwargs)


def linkedin_search(**kwargs):
    return iscraper_request_v2('post', endpoint='linkedin-search', payload=kwargs)


def profile_details(**kwargs):
    return iscraper_request_v2('post', endpoint='profile-details', payload=kwargs)


def profile_details_v3(**kwargs):
    return iscraper_request_v3('get', endpoint='people/profile-details', payload=kwargs)


def profile_activity(**kwargs):
    return iscraper_request_v3('get', endpoint='people/activities', payload=kwargs)


def profile_company_details_v3(**kwargs):
    return iscraper_request_v3('get', endpoint='companies/profile-details', payload=kwargs)


def activity_details(**kwargs):
    return iscraper_request_v3('get', endpoint='people/activity-details', payload=kwargs)