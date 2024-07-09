import datetime
from pprint import pformat, pprint
import json
from typing import Any, Literal
from arbm_core.private.projects import TrackedProject

from sqlalchemy import or_, select

from api_external.iscraper import profile_company_details_v3
from crm.schemas.parsing import LinkedinLikesSignalSchema
from pydantic import HttpUrl, ValidationError

from arbm_core.core.utils import get_one_or_create
from arbm_core.private.linkedin import LinkedinCompany, LinkedinInvestorActivityAssociation, LinkedinLike, LinkedinPersonal, LinkedinPost, LinkedinProfile, LinkedinUrl
from loguru import logger

from projects.schemas.linkedin import ProjectLinkedinDetailsSchema
from projects.schemas.project import ProjectData

from parsing import LinkedinEnrichError

from util import clean_url, get_linkedin_id, nested_get, utc_now


field_mappings = {
    'linkedin_id': [
        ['Id'],
        ['company_id']
    ],
    'linkedin_url': [
        ['CompanyLIUrl'],
        ['urls', 'linkedin_url']
    ],

    'title': [
        ['CompanyName'],
        ['name']
    ],
    'about': [
        ['Description'],
        ['description']
    ],
    'tagline': [
        ['tagline'],
        ['Slogan']
    ],

    'website': [
        ['Website'],
        ['urls', 'company_page']
    ],
    'logo': [
        ['Logo'],
        ['images', 'logo']
    ],
    'crunchbase_url': [
        ['FundingInfo', 'crunchbase-link'],
        ['funding_data', 'organization_url']
    ],
    'category': [
        ['Industries'],
        ['industries', 0, 'name']
    ],
    'industry': [
        ['Industries'],
        ['industries', 0, 'name']
    ],
    'specialities': [
        ['Specialties'],
        ['specialities']
    ],
    'headquarters': [
        ['Headquarters'],
        ['Primary_Loc_Parsed', 'addressCountry'],
        ['locations', 'headquarter'],
        ['other', 0, 'address'],
    ],
    'location': [
        ['Locations', 0, 'Country code'],
        ['locations', 'headquarter', 'country'],
        ['Headquarters'],
        ['Primary_Loc_Parsed', 'addressCountry'],
    ],
    'founded_year': [
        ['Founded'],
        ['founded', 'year']
    ],
    'company_size': [
        ['CompanySize'],
        {'start': ['staff', 'start'], 'end': ['staff', 'end']}
    ],
    'company_size_linkedin': [
        ['EmployeeCountOnLI']
    ],
    'stage': [
        ['FundingInfo', 'last-round-type'],
        ['funding_data', 'last_funding_round', 'funding_type']
    ],
    'last_round_date': [
        ['FundingInfo', 'last-round-date'],
        ['funding_data', 'last_funding_round', 'announced_on']
    ],
    'latest_funding': [
        ['FundingInfo', 'last-round-money-raised'],
        ['funding_data', 'last_funding_round', 'money_raised', 'amount']
    ]
}


def parse_field(field: str, paths: dict | list, data: dict):
    ret = None

    for path in paths:
        if isinstance(path, list):
            if val := nested_get(data, *path, handle_list=True):
                ret = val
                break
        elif isinstance(path, dict):
            ret = {}
            for k, v in path.items():
                val = nested_get(data, *v, handle_list=True)
                if val is None:
                    # if any dict key cannot be retrieved,
                    # then the entire field path is invalid
                    ret = None
                    break

                ret[k] = val

            if ret:
                break
        else:
            raise RuntimeError(f'Invalid path type: {type(path)} for field: {field} '
                                'within company_data field mappings')

    return ret


def parse_company_data(company_data: dict) -> ProjectLinkedinDetailsSchema:
    project_details = {}

    for field, field_paths in field_mappings.items():
        val = parse_field(field, field_paths, company_data)

        # replace uncode 'null' character because it breaks Postgres 'text' type
        if isinstance(val, str):
            val = val.replace('\x00', '')

        project_details[field] = val

    project_details['raw_data'] = json.dumps(project_details)

    return ProjectLinkedinDetailsSchema(**project_details)


def find_leader(s, *,
                leader_urls: list[str],
                leader_ids: list[str]
    ) -> LinkedinPersonal | None:

    if not (any(leader_urls) or any(leader_ids)):
        raise ValueError("at least one from leader_url or leader_id must be provided!")

    filter_expr = or_(LinkedinPersonal.linkedin_id.in_(leader_ids),
                      LinkedinPersonal.urls.any(LinkedinUrl.url.in_(leader_urls))
    )

    return s.scalars(select(LinkedinPersonal).where(filter_expr)).unique().one_or_none()


def load_profile_from_like(s, like: LinkedinLike, profile_details: dict):
    profile_scrambled_url = f"https://linkedin.com/in/{profile_details['profile_id']}/"

    leader = find_leader(s,
                         leader_ids=[like.liker_id, profile_details['profile_id']],
                         leader_urls=[like.profile_url, profile_scrambled_url])

    if leader is None:
        leader = LinkedinPersonal(
            linkedin_id=like.liker_id,
            linkedin_url=f"https://linkedin.com/in/{profile_details['profile_id']}/",
            urls=[
                LinkedinUrl(url=f"https://linkedin.com/in/{profile_details['profile_id']}/"),
                LinkedinUrl(url=like.profile_url)
            ],
        ) # type: ignore

    leader.raw_data = json.dumps(profile_details)
    leader.last_parsed = utc_now()

    leader.name = leader.name or f"{profile_details.get('first_name')} {profile_details.get('last_name')}"
    leader.job = leader.job or profile_details.get('sub_title')

    return leader


def extract_project_data(linkedin_data: ProjectLinkedinDetailsSchema) -> ProjectData:
    # todo: ensure all relevant fields are mapped

    field_mappings = {
        'title': 'title',
        'website': 'website',
        'logo': 'logo',

        'description': 'about',

        'founded': 'founded_year',
        'location': 'location',
        'team_size': 'company_size_linkedin',

        'stage': 'stage',
        'last_round': 'last_round_date',
        'last_round_amount': 'latest_funding',

        'crunchbase_url': 'crunchbase_url',
    }

    # logger.critical(pformat(linkedin_data.dict(exclude_unset=True)))

    # map fields in linkedin data to project fields
    project_details: dict[str, Any] = {'analytics': {}}
    for field, field_path in field_mappings.items():
        val = getattr(linkedin_data, field_path)
        #logger.critical(f'{field}={val}')
        model_fields = ProjectData.schema()['properties']
        if val is not None:
            if field in model_fields:
                project_details[field] = val
            elif field in ProjectData.schema()['definitions'].get('ProjectAnalyticsPatchSchema').get('properties'):
                project_details['analytics'][field] = val
            else:
                raise RuntimeError(f'Invalid field: {field} in field mappings')

    # logger.critical(pformat(project_details))

    if project_details.get('description'):
        project_details['description'] = project_details['description']

    return ProjectData(**project_details)


def update_linkedin_profile(
        s,
        company_url: HttpUrl,
        linkedin_details: ProjectLinkedinDetailsSchema,
    ) -> LinkedinCompany:
    """
    Retrieve or create a company profile from a linkedin source.
    If source doesn't contain linkedin data, fetch from API
    and update the profile with the new data

    :param s: db session
    :param source: linkedin source
    :return: company profile, linkedin details
    """
    # # if company details weren't parsed, load them via API
    # if not linkedin_details:
    #     company_id = get_linkedin_id(company_url, profile_type='company')
    #     linkedin_details = parse_company_data(profile_company_details_v3(profile_id=company_id))

    # find or create linkedin company profile
    company_profile, _ = get_one_or_create(s,
                                        LinkedinCompany,
                                        linkedin_url=company_url,
                                        create_method_kwargs=dict(
                                            name=linkedin_details.title,
                                            linkedin_url=company_url,
                                            linkedin_id=get_linkedin_id(company_url,
                                                                        profile_type='company'),
                                            urls=[
                                                LinkedinUrl(url=company_url)
                                            ],
                                        )
                                    )
    # update linkedin attrs
    for key, val in linkedin_details.dict(exclude_unset=True).items():
        setattr(company_profile, key, val)

        if key == 'raw_data':
            company_profile.last_parsed = utc_now()

    s.add(company_profile)
    s.commit()

    return company_profile


def find_company(s, company_url) -> LinkedinCompany | None:
    return s.query(LinkedinCompany) \
        .filter(LinkedinCompany.linkedin_url == company_url).one_or_none()


def fetch_cached(s, profile_type: Literal['personal', 'company'],
                 max_age: int | None = None, **kwargs) -> dict | None:
    cached_profile = None

    if profile_type == 'personal':
        cached_profile = find_leader(s, **kwargs)
    elif profile_type == 'company':
        cached_profile = find_company(s, **kwargs)
    else:
        raise ValueError(f'invalid profile type {profile_type}')

    if cached_profile and cached_profile.raw_data is not None:
        if max_age and cached_profile.last_parsed < utc_now() - datetime.timedelta(days=max_age):
            logger.debug(f'cached profile data is too old: {cached_profile.last_parsed}')
            return None

        if isinstance(cached_profile.raw_data, dict):

            return cached_profile.raw_data
        try:
            return json.loads(cached_profile.raw_data)
        except json.JSONDecodeError:
            logger.error(f'cached profile data is not valid json: {cached_profile.raw_data}')


def create_post_from_signal(s, signal: LinkedinLikesSignalSchema):
    post, existing = get_one_or_create(s,
                                           LinkedinPost,
                                           post_url=clean_url(signal.post_url),
                                           create_method_kwargs=dict(
                                                like_count=signal.number_of_likes,
                                                relative_post_date=f"{signal.days_since_posted}d",
                                                parsed_date=utc_now()
                                           )
                         )

    return post


def post_add_investor(post, investor, activity_type):
    if investor.id not in [i.investor.id for i in post.investor_interactions]:
        post.investor_interactions.append(LinkedinInvestorActivityAssociation(
            investor=investor,
            activity_type=activity_type,
            discovered_date=utc_now()
        ))
    return post


def find_or_create_leader(s, *, leader_url, leader_name):
    filter_expr = or_(LinkedinPersonal.linkedin_id == leader_id,
                      LinkedinPersonal.urls.any(LinkedinUrl.url == leader_url)
    )

    leader, new = get_one_or_create(s,
                      LinkedinPersonal,
                      filter_expression=filter_expr,
                      create_method_kwargs=dict(
                        name=leader_name,
                        urls=[LinkedinUrl(url=leader_url)],
                      )
            )

    return leader


def get_project_by_linkedin(s, linkedin_url: str) -> TrackedProject | None:
    project_linkedin: LinkedinCompany = s.query(LinkedinCompany).filter(
        LinkedinProfile.urls.any(
            LinkedinUrl.url == linkedin_url
        )).one_or_none()

    if project_linkedin and not project_linkedin.tracked_project:
        raise ValueError(f'found linkedin project {project_linkedin} with url'
                         f' {linkedin_url} but no tracked project associated')

    return project_linkedin.tracked_project if project_linkedin else None
