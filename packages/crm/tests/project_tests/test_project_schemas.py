from pprint import pformat
from loguru import logger
import pytest

from api_external.iscraper import profile_company_details_v3
from projects.linkedin_utils import parse_company_data
from projects.schemas.linkedin import CompanyDetailsSchema


@pytest.fixture
def iscraper_company():
    return profile_company_details_v3(profile_id='grasp-innovations')


rapid_api_data = {
    "Id": 13028574,
    "Logo": "https://media.licdn.com/dms/image/C4D0BAQFLaZRajX1nYw/company-logo_200_200/0/1547735882008?e=2147483647&v=beta&t=kZ_CidXKcddmp-l8ueidZo110jCM1IP66rux4ha_BR4",
    "Type": "Privately Held",
    "Domain": "grasp-innovations.com",
    "Slogan": "We empower aviation security to be agile\n",
    "Founded": "2019",
    "Website": "http://www.grasp-innovations.com",
    "Employees": [
        {
            "Link": "https://nl.linkedin.com/in/jorickvanhees",
            "Name": "Jorick van Hees",
            "Title": "Full stack software engineer (freelance)"
        },
        {
            "Link": "https://nl.linkedin.com/in/robert-schuur-65758019",
            "Name": "Robert Schuur",
            "Title": "Co-Founder at GRASP innovations"
        },
        {
            "Link": "https://nl.linkedin.com/in/josebalizaranzu1982/en",
            "Name": "Joseba Lizaranzu",
            "Title": "Business Intelligence Specialist at GRASP innovations"
        },
        {
            "Link": "https://nl.linkedin.com/in/johannesdiepeveen/en",
            "Name": "Johannes Diepeveen",
            "Title": "CEO & DGA Capturum Software Group (Emendis / Intergrip / D'atalier / GRASP / Sephia)"
        }
    ],
    "Locations": [
        {
            "Address": "Amerlandseweg 14, Breukelen, Utrecht 3621 ZC, NL",
            "Country Code": "NL",
            "Get Directions Link": "https://www.bing.com/maps?where=Amerlandseweg+14+Breukelen+3621+ZC+Utrecht+NL&trk=org-locations_url"
        }
    ],
    "Industries": "IT Services and IT Consulting",
    "CompanyName": "GRASP Innovations",
    "CompanySize": "2-10 employees",
    "Description": "We empower aviation security to be agile\n\nIt’s our business to develop tools that help security professionals make optimal use of available resources and infrastructure in ever-changing circumstances.\n\nWe are called GRASP for a reason\n\nWe use (new) technology-driven data to provide clear insights to optimize the use of resources and infrastructure given any situation.\n\nEnabling security professionals to take immediate action if necessary to get control and solve problems before they arise.\n\n",
    "FundingInfo": {
        "crunchbase-link": "https://www.crunchbase.com/organization/grasp-innovations",
        "last-round-date": "2021-06-27",
        "last-round-type": "Seed",
        "first-listed-investor": "Mainport Innovation Fund",
        "last-round-money-raised": "US$ 670.6K",
        "number-of-funding-rounds": 1
    },
    "Specialties": "security, aviation, process, and technology",
    "CompanyLIUrl": "https://www.linkedin.com/company/grasp-innovations",
    "Headquarters": "Breukelen, Utrecht",
    "FollowerCount": 512,
    "OriginalLIUrl": "https://www.linkedin.com/company/grasp-innovations",

    "Primary_Loc_Parsed": {
        "postalCode": "3621 ZC",
        "addressRegion": "Utrecht",
        "streetAddress": "Amerlandseweg 14",
        "addressCountry": "NL",
        "addressLocality": "Breukelen"
    }
}


def test_api_combatible(iscraper_company):
    company_data = iscraper_company
    logger.error(pformat(company_data))

    model_iscraper = parse_company_data(
        iscraper_company
    ).dict(exclude_unset=True)
    model_rapid_api = parse_company_data(
        rapid_api_data
    ).dict(exclude_unset=True)

    logger.info(pformat(model_iscraper))
    logger.info(pformat(model_rapid_api))

    assert model_iscraper.keys() == model_rapid_api.keys()

    excluded_fields = ['raw_data', 'logo', 'category', 'industry', 'headquarters',
                       'location', 'last_round_date', 'latest_funding']

    for key, value in model_iscraper.items():
        if key in excluded_fields:
            assert value is not None, f"value for {key} must not be none"
            continue

        logger.debug(key)
        assert model_rapid_api[key] == value, f"iScraper.{key} '{value}' != RapidApi.{key} '{model_rapid_api[key]}'"


# def test_rapid_api_data():
    model = parse_company_data(
        rapid_api_data
    )
    logger.info(pformat(model.dict()))