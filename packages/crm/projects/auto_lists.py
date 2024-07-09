import asyncio
import datetime
from collections import defaultdict

from sqlalchemy import select, func
from openai.error import RateLimitError

from arbm_core.private import Session
from arbm_core.private.projects import TrackedProject
from arbm_core.private.investors import Fund
from arbm_core.public.users import ClientOrganization, AutoProjectList, auto_lists_to_projects
from arbm_core.public.projects import Project

from loguru import logger
import pytz

from analysis import AnnotationError
from analysis.annotate_projects import get_company_description
from api_external.openai_api import chat_completion, parse_bool_response
from util import utc_now


def get_active_clients(s):
    return s.scalars(select(ClientOrganization).where(ClientOrganization.membership == 'premium')).all()


def get_active_autolists(org: ClientOrganization):
    logger.info(f'{org}\s autolists:')
    for list in org.auto_project_lists:
        logger.info(f'{list.name}, {list.active}')
    return [autolist for autolist in org.auto_project_lists if autolist.active]


prompt_template = 'You are a skeptical business analyst. Based on its description, decide whether the company {title} ' \
    'matches the provided criteria. YOU MUST ONLY ANSWER TRUE OR FALSE.\n' \
    'Company information:\n' \
    '{company_about}\n\n' \
    'Qualyfying criteria:\n' \
    '{criteria}\n'


def update_org_autolists(s, org):
    autolists = get_active_autolists(org)
    funds: list[Fund] = []

    org_usage = defaultdict(int)

    if not autolists:
        logger.info(f'{org} does not have any active autolsits')
        return

    for f in org.funds_portfolio:
        if fund := s.scalars(select(Fund).where(Fund.uuid == f.uuid)).one():
            funds.append(fund)

    # get companies parsed after threshold
    recent_projects = set()

    runtimes = []
    for al in autolists:
        if al.last_run:
            runtimes.append(al.last_run)

    last_run = min(runtimes).astimezone(pytz.UTC) if runtimes else utc_now() - datetime.timedelta(days=1)
    cutoff = max(last_run, utc_now() - datetime.timedelta(days=1))
    # cutoff = last_run
    logger.info(cutoff)

    for fund in funds:
        # logger.info(f'loading {fund.name}\'s signals')
        signals = fund.compute_signals(cutoff=cutoff)
        # logger.info(f'got {len(signals)} signals')

        for year in signals:
            for month in year.get('months', []):
                # ensure each funds appears only once per month
                for signal in month.get('signals', []):
                    recent_projects.add(signal['project_uuid'])

    logger.critical(f'got {len(recent_projects)} recent projects from {len(funds)} subscribed funds')

    try:
        for project_uuid in recent_projects:
            project = s.scalars(select(TrackedProject).where(TrackedProject.uuid == project_uuid)).one()
            project_about = asyncio.run(get_company_description(project))

            logger.critical(f'checking project {project.title}')

            for autolist in autolists:
                # todo: don't run project if in autolist ? -- only checks for matched projects
                # project_in_list = s.scalars(select(func.count('*')).where(auto_lists_to_projects.c.project_id == project_uuid)).one()
                # logger.info(project_in_list)
                # raise RuntimeError

                prompt = prompt_template.format(title=project.title,
                                                company_about=project_about,
                                                criteria=autolist.prompt)

                # logger.info(prompt)
                try:
                    matches_auto_list, usage = asyncio.run(
                        chat_completion(
                            prompt,
                            response_validator=parse_bool_response,
                            model='gpt-4-1106-preview',
                            include_usage = True
                        )
                    )

                    for token_type, n_used in usage.items():
                        org_usage[token_type] += n_used
                except AnnotationError as e:
                    logger.error(f'unable to annotate project {project}: {e}')
                    break

                # logger.error(f'GPT response: {matches_auto_list}')

                if matches_auto_list:
                    public_project = s.get(Project, project.uuid)
                    autolist.projects.append(public_project)
                    s.commit()

    except RateLimitError:
        logger.error(f'RateLimitError encountered: {e}')
        s.commit()

    for autolist in autolists:
        autolist.last_run = utc_now()
        s.add(autolist)

    s.commit()

    logger.info(f'Used tokens for {org.name}\'s autolists:')
    for token_type, n_used in org_usage.items():
        logger.info(f'\t{token_type}: {n_used}')


def update_lists(s):
    active_orgs = get_active_clients(s)

    for org in active_orgs:
        update_org_autolists(s, org)


if __name__ == "__main__":
    with Session() as s:
        update_lists(s)