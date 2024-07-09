from api_external import ApiError, api_call


def rapid_api_post(endpoint, payload):
    return api_call(method='post',
                    url=endpoint,
                    headers = {
                      'content-type': 'application/json',
                      'X-RapidAPI-Key': '4b58764e65msh0e3112e7acbac0fp1b3103jsnc4f119ae5259',
                      'X-RapidAPI-Host': 'linkedin-company-data.p.rapidapi.com',
                    },
                    params={},
                    data=payload,
                    )


def get_companies_data(company_urls: list[str]) -> dict:
    payload = {"liUrls": company_urls}

    endpoint = f'https://linkedin-company-data.p.rapidapi.com/linkedInCompanyDataJson'
    r = rapid_api_post(endpoint=endpoint, payload=payload)

    response = r.json()
    if response['message'].lower() != 'success':
        raise ApiError('RapidApi has failed',
                       endpoint=endpoint, payload=payload,
                       response_status=r.status_code, response_text=r.text)

    return response
