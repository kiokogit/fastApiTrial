from datetime import datetime
from pprint import pformat, pprint
from typing import Any, Literal
import uuid

from loguru import logger
from pydantic import ValidationError
import pytz
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound

from arbm_core.core.publishing import PublishingError, publish_project
from arbm_core.private.projects import (
    ProjectAnalytics,
    ProjectStatus,
    ProjectLink,
    TrackedProject,
)
from arbm_core.private.investors import Fund
from arbm_core.core.signals import AddSignal, YearMonth
from arbm_core.core.utils import get_one_or_create
from crm import IMAGES_URL
from crm.file_handlers import save_project_thumbnail

from projects import FilteringEvent, FilteringException, ProjectEvent, ProjectException, ProjectInitError
from projects.linkedin_utils import update_linkedin_profile
from projects.project_filtering import filter_b2b_company, filter_is_startup, filter_signal, filter_company
from projects import (
    DuplicateProjectsError,
)
from projects.schemas.filters import FilterResult

from projects.schemas.project import ProjectData

from crm.schemas.projects import ProjectCreateSchema, ProjectSchema
from projects.schemas.signals import FundIdSchema, LinkedinSourceSchema, ProjectSourceSchema
from util import get_url_root


def update_entities(s, project_update: ProjectData, source: ProjectSourceSchema) -> Any:
    match source:
        case LinkedinSourceSchema():
            # get or create linkedin profile, parse company data
            try:
                company_profile = update_linkedin_profile(s,
                                                      source.company_url,
                                                      source.linkedin_details)
            except MultipleResultsFound as e:
                raise DuplicateProjectsError(f'multiple projects found with website {source.company_url}')

            # enforce no duplicate companies
            if (parent := company_profile.tracked_project):
                if not parent.website:
                    logger.error(f'company {company_profile} already has a' \
                                           f'parent project {parent}, but the parent has no website')
                    # raise ProjectInitError(f'company {company_profile} already has a' \
                    #                        f'parent project {parent}, but the parent has no website')
                elif get_url_root(parent.website) != get_url_root(project_update.website):
                    raise ProjectInitError(f'company {company_profile} already has a ' \
                                           f'parent project {parent} with url "{parent.website}", ' \
                                           'which is not the same as the signal url ' \
                                           f'"{project_update.website}"' \
                                           f'\n\n({get_url_root(parent.website)} != {get_url_root(project_update.website)})')

            return company_profile
        case _:
            raise NotImplementedError


def update_project_details(project: TrackedProject,
                           project_data: ProjectData
                        ) -> TrackedProject:
    project_details = project_data.dict(exclude_unset=True)

    if 'description' in project_details:
        project.analytics.update_detail(attr_name='Description',
                                    new_value=project_details['description'],
                                    data_source='linkedin')
        del project_details['description']

    if 'crunchbase_url' in project_details:
        if not project.get_link('crunchbase_url'):
            project.links.append(
                ProjectLink(
                    name='crunchbase_url',
                    value=project_details['crunchbase_url']
                ) # type: ignore
            )
        del project_details['crunchbase_url']

    logger.critical(f'updating project with id {project.id} with details {pformat(project_details)}')
    logger.critical(f'project website is {project.website}')

    for attr, val in project_details.items():
        if attr == 'website':
            continue

        if attr == 'logo':
            project.logo = IMAGES_URL + '/' + str(save_project_thumbnail(project.uuid, project_details['logo']))

        # set nested attributes, e.g. ProjectAnalytics recursively
        logger.info(f'updating project attribute {attr} to {val}')

        if isinstance(val, dict):
            complicated_attr = getattr(project, attr)

            logger.info(f'got complicated attr {complicated_attr} for name {attr}')

            for sub_attr, sub_val in val.items():
                setattr(complicated_attr, sub_attr, sub_val)
            setattr(project, attr, complicated_attr)
            continue

        # set simple attributes
        if val is not None:
            setattr(project, attr, val)

    return project


def update_category(s, project: TrackedProject, category: str, status: bool):
    if category == 'startup':
        project.is_startup = status
    elif category == 'b2b':
        project.is_b2b = status

    s.add(project)
    s.commit()

def reject(s, project, type):
    update_category(s, project, type, False)


def accept(s, project, type):
    update_category(s, project, type, True)


def filter_b2b_signals(s,
            signals: list[FundIdSchema],
            project_data: ProjectData,
            source: ProjectSourceSchema,
            events_group: uuid.UUID,
            events_queue: list,
            project: TrackedProject,
            exists: bool
        ) -> tuple[list[FundIdSchema], list]:

    is_valid_competitor = filter_b2b_company(s, project_data)
    is_valid_competitor_event = FilteringEvent(
            group_id=events_group,
            event_name='filter_is_valid_competitor_passed' if is_valid_competitor.passed else 'filter_is_valid_competitor_failed',
            display_name='Company is a valid competitor' if is_valid_competitor.passed else 'Company is not a valid competitor',
            data=[is_valid_competitor]
    )

    if not is_valid_competitor.passed:
        reject(s, project, 'b2b')
        raise FilteringException(f'company is not a valid competitor',
                    cause=is_valid_competitor_event)

    events_queue.append(is_valid_competitor_event)

    signals_passed = signals
    return signals_passed, []


def filter_startup_signals(s,
            signals: list[FundIdSchema],
            project_data: ProjectData,
            source: ProjectSourceSchema,
            events_group: uuid.UUID,
            events_queue: list,
            project: TrackedProject,
            exists: bool
        ) -> tuple[list[FundIdSchema], list]:

    if project.is_startup is not None:
        is_startup_event = FilteringEvent(
                group_id=events_group,
                event_name='filter_is_startup_passed_historic' if project.is_startup else 'filter_is_startup_failed_historic',
                display_name='Company was previously classified as startup' if project.is_startup else 'Company was previously classified as NOT a a startup',
                data=[],
                project_data=ProjectSchema.from_orm(project)
        )
        if project.is_startup == False:
            reject(s, project, 'startup')
            raise FilteringException('company is not a startup',
                                    cause=is_startup_event)
    else:
        is_startup = filter_is_startup(s, project_data)
        is_startup_event = FilteringEvent(
                group_id=events_group,
                event_name='filter_is_startup_passed' if is_startup.passed else 'filter_is_startup_failed',
                display_name='Company is a valid startup' if is_startup.passed else 'Company is not a startup',
                data=[is_startup]
        )

        if not is_startup.passed:
            reject(s, project, 'startup')
            raise FilteringException('company is not a startup',
                                    cause=is_startup_event)

    events_queue.append(is_startup_event)

    company_filters_result: FilterResult = filter_company(s, project_data)
    startup_passed_filters_event = FilteringEvent(
                                    group_id=events_group,
                                    event_name='company_filter_passed' if company_filters_result.passed else 'company_filter_failed',
                                    display_name='Company passed filters' if company_filters_result.passed else 'Company did not pass filters',
                                    data=[company_filters_result]
                                )

    if not company_filters_result.passed:
        reject(s, project, 'startup')
        raise FilteringException(f'company "{project_data.title}" did not pass filters',
                                cause=startup_passed_filters_event)

    signal_filter_results: list[FilterResult] = []
    signals_passed: list[FundIdSchema] = []

    for signal in signals:
        # if project already exists, check if signal has already passed
        # in the past
        if exists and signal.id in [f.id for f in project.funds]:
            events_queue.append(FilteringEvent(
                group_id=events_group,
                event_name='filter_signal_passed_historic',
                display_name='Signal matched in the past',
                data=[],
                project_data=ProjectSchema.from_orm(project)
            ))

            signals_passed.append(signal)
            continue

        signal_filter_results.append(out := filter_signal(s, signal, project_data))

        events_queue.append(FilteringEvent(
                group_id=events_group,
                event_name='filter_signal_passed' if out.passed else 'filter_signal_failed',
                display_name='Signal matched' if out.passed else 'Signal did not match',
                data=[out]
        ))

        if out.passed:
            signals_passed.append(signal)

    return signals_passed, signal_filter_results


def inject_project(s,
            signals: list[FundIdSchema],
            project_data: ProjectData,
            source: ProjectSourceSchema,
            events_group: uuid.UUID,
            events_queue: list,
            project_type: Literal['startup', 'b2b']
        ) -> TrackedProject:
    if project_type not in ('startup', 'b2b'):
        raise ValueError(f'project type must be either "startup" or "b2b", got {project_type}')

    # update or create source entitiy, e.g. LinkedinCompany
    # if entity is already linked to project, check the website
    # and raise error if different
    entity_profile = update_entities(s, project_data, source)
    s.commit()

    # find or create project
    try:
        create_schema = ProjectCreateSchema(
                        project_type=('competitors' if project_type == 'b2b' else 'startup'),
                        **project_data.dict()
                    )
    except ValidationError as e:
        raise ProjectException(
                    'project failed data validation for CreateSchema',
                    cause=ProjectEvent(
                        group_id=events_group,
                        event_name='validation_failed',
                        display_name='Project data failed validation',
                        data={
                            'project_data': project_data.dict(),
                            'errors': e.errors()
                        }
                    ))

    try:
        project, exists = get_one_or_create(s, TrackedProject,
                website=create_schema.website,
                create_method_kwargs={
                    **create_schema.dict(exclude_unset=True),
                    'analytics': ProjectAnalytics(**create_schema.analytics.dict(exclude_unset=True)),
                }
            )

        logger.info(f'project with title "{project.title}", website "{project.website}" {"created" if not exists else "updated"}')
    except MultipleResultsFound:
        raise DuplicateProjectsError(f'multiple projects found with website {create_schema.website}')

    if not project.analytics:
        project.analytics = ProjectAnalytics()

    s.add(project)
    s.commit()

    # set entity to track the project, and update project data
    if entity_profile is not None:
        entity_profile.tracked_project = project

    if project_data:
        project = update_project_details(project, project_data)

    s.add(project)
    s.commit()

    if exists:
        # careful here! value None is not the same as False
        if project_type == 'startup' and project.is_startup is False:
            raise ProjectException(f'project with website "{project.website}" already exists, but is not a startup',
                                    cause=ProjectEvent(event_name='project_rejected', display_name='Project was previously rejected',
                                                       details=f'project with website "{project.website}" already exists, but was previously qualified as NOT a startup',
                                                       group_id=events_group, project_id=project.id, data={'project_data': project_data.dict(exclude_unset=True)})
                                    )
        elif project_type == 'b2b' and project.is_b2b is False:
            raise ProjectException(f'project with website "{project.website}" already exists, but is not a b2b company',
                                   cause=ProjectEvent(event_name='project_rejected', display_name='Project was previously rejected',
                                                       details=f'project with website "{project.website}" already exists, but was previously qualified as NOT a b2b company',
                                                       group_id=events_group, project_id=project.id, data={'project_data': project_data.dict(exclude_unset=True)})
                                    )

    if project_type == 'startup':
        signals_passed, signal_filter_results = filter_startup_signals(s, signals, project_data, source, events_group, events_queue, project, exists)
    elif project_type == 'b2b':
        signals_passed, signal_filter_results = filter_b2b_signals(s, signals, project_data, source, events_group, events_queue, project, exists)
    else:
        raise NotImplementedError

    # filter to check if this is a valid signal
    # todo: use project data object here

    if not any(signals_passed):
        raise FilteringException(
                                f'no signal matched against the fund\'s thesis from the set: {signals}',
                                cause=FilteringEvent(
                                    group_id=events_group,
                                    event_name='no_signal_matched',
                                    display_name='No signals matched',
                                    details='no signal matched against any of the funds\' theses',
                                    data=signal_filter_results
                                ))

    s.add(project)
    s.commit()

    # add signals by fund to the project
    for signal in signals_passed:
        fund = s.scalars(select(Fund).where(Fund.id == signal.id)).unique().one()

        project.add_signal(signal=AddSignal(
                                project_uuid=project.uuid,
                                fund_uuid=fund.uuid,
                                timeframe=YearMonth(
                                    year=source.signal.picked_up_date.year,
                                    month=source.signal.picked_up_date.month,
                                    # year=datetime.now(tz=pytz.UTC).year,
                                    # month=datetime.now(tz=pytz.UTC).month,
                                ),
                                #todo: post id
                                source=source
                            )
                        )

    # update project status if not accepted or rejected
    if project.status not in (ProjectStatus.accepted, ProjectStatus.rejected):
        project.status = ProjectStatus.review

    accept(s, project, project_type)

    s.add(project)
    s.commit()

    try:
        publish_project(project_uuid=project.uuid, require_details_fields=False)
    except PublishingError as e:
        logger.error(f'project {project} failed to publish:\n{e}')

    logger.info(f'{project} with website "{project_data.website}" was ' \
                f'{"created" if not exists else "updated"} from signal')

    events_queue.append(
        ProjectEvent(
            group_id=events_group,
            event_name='project_created' if not exists else 'project_updated',
            display_name='Project created' if not exists else 'Project updated',
            project_id=project.id,
            data={'project_data': project_data.dict(exclude_unset=True), 'signal_added': signals_passed}
        )
    )

    return project
