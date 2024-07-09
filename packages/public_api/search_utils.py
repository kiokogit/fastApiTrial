from typing import Callable
from loguru import logger
from uuid import UUID

from pydantic import ValidationError
from fastapi import HTTPException
from sqlalchemy import and_, func, or_
from sqlalchemy.exc import IntegrityError

from arbm_core.core.publishing import publish_project, PublishingError
from arbm_core.private.projects import TrackedProject, ProjectAnalytics, ProjectTagsAssociation, ProjectTag, ProjectStatus, InvestmentStage

from schemas.schemas import SearchFiltersSchema, BusinessModel, ProductType

from dependencies import PrivateSession
from utils import clean_query_string


filters_config = {
        'keywords': {'type': 'suggest', 'filter_mode': 'broad'},
        'industry': {'type': 'suggest', 'filter_mode': 'broad'},
        'origin_country': {'type': 'suggest', 'filter_mode': 'refined'},

        # constants
        'verticals': {
            'type': 'constants',
            'filter_mode': 'broad_custom',
            'values': [
                    "Energy Infrastructure",
                    "Agriculture",
                    "Airport",
                    "Alternative Fuel",
                    "Automotive Finance",
                    "Autonomous Vehicles",
                    "Aviation",
                    "Battery",
                    "Battery Testing & Diagnostics",
                    "Car",
                    "Car Sales",
                    "Car Subscription",
                    "Charging Infrastructure",
                    "Connected Car",
                    "Delivery",
                    "Driving Assistance",
                    "Drones",
                    "Electric Vehicle (EV)",
                    "Energy Management",
                    "Fleet Management",
                    "Fueling Stations",
                    "Heavy Vehicle",
                    "Hydrogen",
                    "Infotainment",
                    "Insurance",
                    "Multi-Modal Mobility",
                    "Leasing Assets",
                    "Logistics",
                    "Maintenance",
                    "Maritime Transportation",
                    "Micro-Mobility",
                    "Mobility Hub",
                    "Navigation",
                    "OEM Focussed",
                    "Parking",
                    "Public Transportation",
                    "Rail Transportation",
                    "Range Extender",
                    "Ride Hailing",
                    "Roadside Assistance",
                    "Robotics",
                    "Sensing",
                    "Supply Chain",
                    "Traffic Management",
                    "Urban Planning",
                    "Vehicle"
            ]
        },
        'customer_segments': {
            'type': 'constants',
            'filter_mode': 'broad_custom',
            'sort': 'manual',
            'values': [
                    'Individuals and Consumers',
                    'Small & Medium Businesses',
                    'Large Corporations',
                    'Government & Public Organizations',
            ]
        },
        'competing_spaces': {
            'type': 'constants',
            'filter_mode': 'refined',
            'values': [
                    'Transportation and Mobility',
                    'Energy and Sustainability',
                    'Data and Artificial Intelligence',
                    'Health and Wellness',
                    'Agriculture Food Processing and Green Technologies',
                    'Financial Services',
                    'Insurance and Fintech',
                    'Aerospace and Space Exploration',
                    'Advertising and Media',
                    'Business Services and Consulting'
            ]
        },

        'funding_stage': {
            'type': 'constants',
            'filter_mode': 'refined_always_or',  # these values are mutually exclusive hence there are no meaningful combinations for them
            'sort': 'manual',
            'values': [s for s in InvestmentStage]},
        'product_types': {
            'type': 'constants',
            'filter_mode': 'refined',
            'sort': 'manual',
            'values': [t for t in ProductType]},
        'business_models': {
            'type': 'constants',
            'filter_mode': 'refined',
            'sort': 'manual',
            'tag_type': 'company_types',
            'query_val': lambda x: x.name,
            'values': [c for c in BusinessModel]
            },
    }


def search_values(s, query_str: str, orm_model, search_field: str, pre_filters: list | None = None, max_results: int = 20):
    tokens = query_str.split()

    logger.critical(tokens)
    subqueries = []
    for token in tokens:
        query_token = f'%{token}%'
        subqueries.append(query_token)

    query_filter = or_(*[getattr(orm_model, search_field).ilike(subquery) for subquery in subqueries])
    if pre_filters is not None:
        query_filter = and_(*pre_filters, query_filter)

    matching_entries = s.query(orm_model).filter(
        query_filter
    ).all()

    logger.critical(matching_entries)

    # reverse=False required for aphabetical sort, so we invert boolean check for field starting with token,
    # so that True values become False and get sorted at the beginning (default sorting is in ascending order)
    sorted_entries = sorted(matching_entries, key=lambda entry: (not getattr(entry, search_field).lower().startswith(tokens[0]), getattr(entry, search_field).lower()))

    return sorted_entries[:max_results]


def build_filter_multiple(values: list, filter_factory: Callable):
    filter_instances = [filter_factory(v) for v in values]
    return filter_instances


def generate_filters(supplied_filters: SearchFiltersSchema):
    def get_tag_filter_factory(tag_type: str):
        tag_conf = filters_config[tag_type]

        tag_type = tag_conf['tag_type'] if 'tag_type' in tag_conf else tag_type
        query_val_fn = (lambda x: (tag_conf['query_val'](x)).lower()) if 'query_val' in tag_conf else (lambda x: x.lower())

        return lambda x: TrackedProject.analytics.has(ProjectAnalytics.tags.any(
                and_(ProjectTagsAssociation.tag_type == tag_type,
                     func.lower(ProjectTagsAssociation.tag_name) == query_val_fn(x))
            ))


    broad_filters = []
    refined_filters = []

    # ==== BROAD SEARCH FILTERS =====
    # these filters can be AND or OR
    broad_filter_names = [filter_name for filter_name, conf in filters_config.items() if conf['filter_mode'] == 'broad']

    verticals = supplied_filters.verticals
    segments = supplied_filters.customer_segments

    broad_filters.append(and_(
        or_(
            *build_filter_multiple(
                values=verticals or [],
                filter_factory=get_tag_filter_factory('verticals')
            ),
        ),
        or_(
            *build_filter_multiple(
                values=segments or [],
                filter_factory=get_tag_filter_factory('customer_segments')
            )
        )
    ))

    for filter_field in broad_filter_names:
        if vals := getattr(supplied_filters, filter_field, None):
            broad_filters.extend(
                build_filter_multiple(
                    values=vals,
                    filter_factory=get_tag_filter_factory(filter_field)
                )
            )

    # ==== REFINED SEARCH FILTERS =====
    # these filters are always applied alongside broad search filters

    # ANY of the options should be present
    if stages := supplied_filters.funding_stage:
        refined_filters.append(
            or_(*build_filter_multiple(
                values=stages,
                filter_factory=lambda x: TrackedProject.analytics.has(ProjectAnalytics.stage == x)
            ))
        )

    # multi-value filters, all values MUST be present, e.g. software AND hardware
    refined_filter_names = [filter_name for filter_name, conf in filters_config.items() if conf['filter_mode'] == 'refined']
    for filter_field in refined_filter_names:
        vals = getattr(supplied_filters, filter_field, None)
        if filter_field == 'competing_spaces':
            vals = getattr(supplied_filters, 'competing_space', None)

        if vals:
            refined_filters.extend(
                build_filter_multiple(
                    values=vals,
                    filter_factory=get_tag_filter_factory(filter_field)
                )
            )

    if origin_country := supplied_filters.origin_country:
        refined_filters.append(TrackedProject.analytics.has(ProjectAnalytics.location == origin_country))

    # founded dates must always be AND, otherwise all projects are found
    # e.g. (filters) OR (founded < 2023 OR founded > 2017)
    filters_founded = []
    if founded_from := supplied_filters.founded_from:
        filters_founded.append(ProjectAnalytics.founded >= founded_from)
    if founded_to := supplied_filters.founded_to:
        filters_founded.append(ProjectAnalytics.founded <= founded_to)
    if filters_founded:
        refined_filters.append(TrackedProject.analytics.has(and_(*filters_founded)))

    return broad_filters, refined_filters


def build_query(s, search_mode: str, filter_values: SearchFiltersSchema):
    search_mode = search_mode.lower()
    if not search_mode in ['and', 'or']:
        raise ValueError('incorrect filter type, should be either and or or')

    broad_filters, refined_filters = generate_filters(filter_values)

    if not broad_filters and not refined_filters:
        raise HTTPException(status_code=400, detail="at least one filter must be supplied!")

    filter_stmt = None
    if search_mode == 'or':
        filter_stmt = or_(*broad_filters)
    elif search_mode == 'and':
        filter_stmt = and_(*broad_filters)

    filter_stmt = and_(filter_stmt, *refined_filters)
    return s.query(TrackedProject.uuid).filter(TrackedProject.status == ProjectStatus.accepted).filter(filter_stmt)


def lookup_project(s, q: str, limit: int = 10, include_pending: bool = False) -> list[TrackedProject]:
    project_query = clean_query_string(q)
    filter_status = [TrackedProject.status == ProjectStatus.accepted]

    if include_pending:
        filter_status.append(TrackedProject.status.in_([ProjectStatus.review, ProjectStatus.published]))

    return search_values(s, project_query,
                  TrackedProject,
                  search_field='title',
                  pre_filters=[or_(*filter_status)],
                  max_results=limit)


def suggest_projects(s, q: str, include_data: bool = False, include_pending: bool = False) -> list[dict]:
    likely_projects: list[TrackedProject] = lookup_project(s, q, include_pending=include_pending)

    if include_data:
        return [{'title': p.title,
                'uuid': p.uuid,
                'description': p.description,
                'website': p.website} for p in likely_projects]

    return [{'title': p.title, 'uuid': p.uuid} for p in likely_projects]


def search_projects(s,
                    search_mode: str,
                    filter_values: SearchFiltersSchema
    ):

    query = build_query(s, search_mode=search_mode, filter_values=filter_values)
    res = query.limit(1000).all()
    uuids = [p.uuid for p in res]

    return uuids


def search_and_publish(*, private_s, search_filters: SearchFiltersSchema):
    project_uuids = search_projects(private_s, search_mode=search_filters.search_mode, filter_values=search_filters)

    logger.critical(f'found {len(project_uuids)} projects')
    for p_uuid in project_uuids:
        try:
            publish_project(project_uuid=p_uuid)
        except (PublishingError, ValidationError):
            logger.critical('could not publish project -- discovered date not found or validation failed')
        except IntegrityError:
            logger.critical('could not publish project -- integrity error, project might be already published')

    return project_uuids


def suggest_tag_values(s, filter_field: str, query_str: str):
    tag_filter_config = filters_config[filter_field]

    tag_type = tag_filter_config['tag_type'] if 'tag_type' in tag_filter_config else filter_field
    suggested_options = search_values(s, query_str, ProjectTag, 'name', pre_filters=[ProjectTag.type == tag_type])

    suggested_items = [getattr(option, 'name') for option in suggested_options]
    return suggested_items


def copy_project_filters(session, project_uuid: UUID) -> dict[str, list | None]:
    target_project: TrackedProject = session.query(TrackedProject) \
        .filter(TrackedProject.uuid == project_uuid).one_or_none()

    if target_project is None or not target_project.status == ProjectStatus.accepted:
        raise HTTPException(status_code=404, detail=f'project not found with uuid {project_uuid}')

    if not target_project.analytics:
        raise HTTPException(status_code=404, detail=f'project {project_uuid} does not have analytics data')

    if not target_project.analytics.tags:
        raise HTTPException(status_code=404, detail=f'project {project_uuid} does not have tags')

    analytics: ProjectAnalytics = target_project.analytics

    enum_tag_types = ['product_type', 'customer_type']
    enum_values = {}

    for tag_type in enum_tag_types:
        enum_values[tag_type] = [tag.tag_name for tag in analytics.tags if tag.tag_type == tag_type]

    for tag_type in enum_tag_types:
        tag_value = enum_values.get(tag_type, [])
        if len(tag_value) > 1:
            raise HTTPException(status_code=400, detail='more than one value found for tag of type enum!')
        if len(tag_value) == 1:
            enum_values[tag_type] = tag_value[0]

    remap_filters = {
        'company_types': 'business_models',
        'competing_space': 'competing_spaces'
    }

    project_filters = {}
    for tag_type in ['keywords', 'verticals', 'customer_segments', 'competing_space', 'origin_country',
                        'product_types', 'company_types']:
        project_filters[tag_type] = [tag.tag_name for tag in analytics.tags if tag.tag_type == tag_type]

    remapped_project_filters = {}
    for k, v in project_filters.items():
        if k == 'company_types':
            new_v = []
            logger.critical(v)
            for key in v:
                new_v.append(BusinessModel[key.lower()].value)
            v = new_v

        if k in remap_filters:
            remapped_project_filters[remap_filters[k]] = v
        else:
            remapped_project_filters[k] = v

    remapped_project_filters['funding_stage'] = [analytics.stage] if analytics.stage else None

    return remapped_project_filters


def get_filter_values(session, field_name: str, query_str: str | None = None):
    if field_name not in filters_config:
        raise HTTPException(status_code=400, detail='unsupported field name')

    filter_config = filters_config[field_name]

    if filter_config['type'] == 'constants':
        if filter_config.get('sort') == 'manual':
            return filter_config['values']
        else:
            return sorted(filter_config['values'])

    if query_str is None:
        raise HTTPException(status_code=400, detail='query string must be provided to search for suggested options')

    query_str = clean_query_string(query_str)
    return suggest_tag_values(session, field_name, query_str)
