import asyncio
import math
import re
from json import JSONDecodeError
from pprint import pformat, pprint
from typing import List, Literal, Tuple
from uuid import UUID
import sys
import os

from loguru import logger

from sqlalchemy import func, select, and_, inspect
from sqlalchemy.exc import IntegrityError
from analysis.statistics import get_untagged_projects

from arbm_core.core import MongoDb
from arbm_core.core.signals import get_unique_signals_for_fund

from arbm_core.private import Session
from arbm_core.private.linkedin import LinkedinCompany
from arbm_core.private.projects import (
    ProjectStatus,
    ProjectAnalytics,
    ProjectTag,
    TrackedProject,
    ProjectTagsAssociation,
    DetailsEntry
)
from packages.crm.projects.publishing import publish_project
from arbm_core.public.projects import Project

import util
from analysis import AnnotationError
from api_external.openai_api import chat_completion, parse_json_response
from parsing.web_scraper import parse_website
from util import read_const_file

from analysis.gpt_tag import GPTTagger

LLAMA_API_KEY = os.environ['LLAMA_API_KEY']


async def get_company_description(project: TrackedProject, require_all=False):
    analytics = project.analytics or ProjectAnalytics()

    if analytics.details is None:
        analytics.details = []

    website_url = project.website
    if not website_url:
        if project.linkedin_profile:
            website_url = project.linkedin_profile.website
    if not website_url:
        for entry in analytics.details:
            if entry.data_source == 'crunchbase' and entry.type == 'Website':
                website_url = entry.value
                break

    description_fields = ['Description', 'description']
    for field in description_fields:
        if (description := analytics.get_attr(field, 'detail')) and (description.value):
            about = description.value
            break

    # logger.debug(f'project.description: {project.description}')

    if not about:
        if project.linkedin_profile:
            about = project.linkedin_profile.about
            # logger.debug(f'project.linkedin_profile.about: {project.linkedin_profile.about}')

    hq = analytics.location
    if not hq:
        hq = getattr(getattr(project, 'linkedin_profile', {}), 'headquarters', None)
        if not hq:
            for entry in analytics.details:
                if entry.data_source == entry.type == 'Headquarters Location':
                    hq = entry.value
                    break

    web_content = ''
    for entry in analytics.details:
        if entry.type == 'website_content':
            web_content = entry.value
            break

    # logger.debug(f'web content: "{web_content}"')
    # if web_content.strip() == '' and website_url:
    #     logger.debug('trying to parse website content')

    #     web_content = await parse_website(website_url)

    #     if web_content:
    #         analytics.update_detail(attr_name='website_content', new_value=web_content, data_source='web_scrapper')
    #     else:
    #         logger.error(f'unable to parse website content for url {website_url}')

    valid_descriptions = [text.strip() != '' for text in (about, web_content) if text is not None]

    if (not any(valid_descriptions)) or\
            (require_all and not all(valid_descriptions)):
        raise AnnotationError(f'valid description for project is not provided')

    founders = ''
    if analytics.details:
        founders = ', '.join([d for d in analytics.details if d.type == 'Founders'])
    if analytics.leaders:
        founders += ','.join([l.name for l in analytics.leaders if l.name])

    about = about
    website_content = web_content
    founders = founders or "N/A;"
    headquarters = hq or "N/A;"

    # logger.debug(about)

    company_data_template = read_const_file('openai/company_data.txt').read_text()
    return company_data_template.format(**{
        'project': project,
        'about': about,
        'founders': founders,
        'headquarters': headquarters,
        'website_content': website_content,
    })


def upsert_tag(s, type, value):
    tag = s.query(ProjectTag).filter(func.lower(ProjectTag.type) == type.lower(),
                                     func.lower(ProjectTag.name) == value.lower()) \
        .one_or_none()
    logger.info(f'found tag for {type} - {value}: {tag}')
    return tag or ProjectTag(type=type, name=value)


async def update_tag(project, tag_type, tag_values, source: str):
    logger.debug(f'processing {tag_type} annotation')

    # task_names, allowed_tag_values: dict | None = None
    # if tag_type not in task_names:
    #     raise AnnotationError(f"unexpected tag name: {tag_type}")

    # if not isinstance(tag_values, (list, str)):
    #     logger.error(pformat(company_tags))
    #     raise AnnotationError(f"unexpected type of the tag value: {type(tag_values)}")

    # values = tag_values if isinstance(tag_values, list) else [tag_values]
    # logger.debug(f'\ttag value: {tag_values}')

    # if tag_type in allowed_tag_values:
    #     invalid_v_ids = [i for i, v in enumerate(values) if v not in allowed_tag_values[tag_type]]

    #     if any(invalid_v_ids):
            # logger.error(f"unexpected(s) value for tag {tag_type}: {[values[i] for i in invalid_v_ids]}, allowed values: {allowed_tag_values[tag_type]}")

            # values = await gpt_correct_task(tag=tag_type, project=project, response_values=values, allowed_values=allowed_tag_values[tag_type])

    analytics: ProjectAnalytics = project.analytics or ProjectAnalytics(project=project)

    if s := inspect(project).session:
        s.add(analytics)

    try:
        analytics.update_tags(tag_type, tag_values, data_source=source)
    except IntegrityError as e:
        logger.error(f'error updating tag {tag_type}')
        logger.error(e)


def set_project_details(project, company_details, label_names, label_values: list | None = None):
    for label, value in company_details.items():
        if label not in label_names:
            logger.error(pformat(company_details))
            raise AnnotationError(f"unexpected label name: {label}")
        if label_values and (value not in label_values):
            logger.error(pformat(company_details))
            raise AnnotationError(f"unexpected label value: {value}")

        if isinstance(value, list):
            value = list[0]

        analytics: ProjectAnalytics = project.analytics
        analytics.update_detail(label, value, 'gpt3.5')


async def gpt_correct_task(tag, project, response_values, allowed_values):
    company_data = await get_company_description(project)

    description = f"Company description: {company_data}"

    prompt = f"I am reviewing the company {project.title} which has this information about it: {description};"\
             f"\nI have got the following list of values representing invalid {tag} options for the company {project.title}: {response_values}" \
             f"\nBased on the information about the company, choose the same number of options from the list below" \
             f" that are closest to the existing invalid options: \"{allowed_values}\";" \
             f"\n!Provide your response as a valid json array containing ONLY the new values!"

    pprint(prompt)
    response = await chat_completion(prompt)
    logger.critical(response_values)
    logger.critical(response)
    try:
        new_vals = parse_json_response(project, response)
    except JSONDecodeError:
        new_vals = ["Other"]

    if not isinstance(new_vals, list) or not len(new_vals) > 0 or not all([v in allowed_values for v in new_vals]):
        return ["Other"]
        raise AnnotationError(f"unexpected value(s) for tag {tag}:"
                              f" {response_values}, allowed values: {allowed_values}, and refined parsing failed")

    return new_vals


async def annotate_company_descriptive(s, project: TrackedProject, company_data: str,
                                 parsed_tags: list[str], parsed_details: list[str]):
    categorical_values = dict(
                    verticals = util.read_list('openai/verticals.txt'),
                    industries = util.read_list('openai/industries.txt'),
                    competing_space = util.read_list('openai/competing_spaces.txt'),
                    customer_segments = util.read_list('openai/customer_segments.txt'),
                    product_types = ['Pure Software', 'Hardware & Software'],
                    company_types = ['B2B', 'B2C', 'C2C'],
    )

    tasks_categorical = read_const_file('openai/categorical_tasks.txt').read_text()

    qualitative_tags, qualitative_unstructured = read_const_file('openai/qualitative_tags.txt').read_text(), \
                                                 read_const_file('openai/qualitative_unstructered.txt').read_text()
    tasks_qualitative = '\n'.join([qualitative_tags, qualitative_unstructured])

    valid_tag_types = [re.search(r'^(\w+):', t).group(1) for t in
                           '\n'.join([tasks_categorical, qualitative_tags]).split('\n')
                           if re.search(r'^(\w+):', t)]
    valid_details_types = [re.search(r'^(\w+):', t).group(1) for t in qualitative_unstructured.split('\n')
                           if re.search(r'^(\w+):', t)]

    skipped_tasks = parsed_details + parsed_tags
    logger.debug('skipping tasks:', skipped_tasks)

    # remove tasks which are already done
    tasks_qualitative = '\n'.join([t for t in tasks_qualitative.split('\n')
                                    if re.search(r'^(\w+):', t)
                                    and re.search(r'^(\w+):', t).group(1) not in skipped_tasks])
    tasks_categorical = '\n'.join([t for t in tasks_categorical.split('\n')
                                   if re.search(r'^(\w+):', t)
                                   and re.search(r'^(\w+):', t).group(1) not in skipped_tasks])

    # append categories for tasks which are being run
    for data_piece in read_const_file('openai/categorical_data.txt').read_text().split('\n'):
        if (re.search(r'\{(\w+)\}', data_piece))\
            and (task_name := re.search(r'\{(\w+)\}', data_piece).group(1))\
            and task_name not in skipped_tasks:
            tasks_categorical += f'\n\n{data_piece}'

    annotation_tags = {}
    annotation_details = {}
    for task_template in [tasks_categorical, tasks_qualitative]:
        if not task_template:
            logger.debug(f'skipping task template {task_template}')
            continue

        task_template = task_template.format(**dict(
            company=project.title,
            # insert the categorical data
            **{k: ', '.join(v) for k, v in categorical_values.items()},
        ))

        descriptive_prompt = read_const_file('openai/descriptive_prompt.txt').read_text()
        prompt = descriptive_prompt.format(
            tasks=task_template,
            company_data=company_data,
        )

        logger.debug('prompt:')
        logger.debug(pformat(prompt))
        tasks_json = await chat_completion(prompt)

        try:
            company_annotations = parse_json_response(project, tasks_json)
        except JSONDecodeError:
            return

        logger.debug(pformat(tasks_json))

        for k, v in company_annotations.items():
            logger.critical(k)
            # skip items that were already parsed
            if (k in valid_tag_types and k in parsed_tags) \
                    or (k in valid_details_types and k in parsed_details):
                logger.debug(f'skipping annotation of type {k}')
                continue

            if k in valid_tag_types:
                annotation_tags[k] = v
            elif k in valid_details_types:
                annotation_details[k] = v
            else:
                logger.error(f"unsupported annotation type: {k}")
                #raise AnnotationError(f"unsupported annotation type: {k}")

    for tag, tag_values in annotation_tags.items():
        await update_tag(project, tag, tag_values, source='gpt3.5')

    set_project_details(project, annotation_details, valid_details_types)


async def tag_with_tagger(project_description: str, tagger: GPTTagger, number_of_tries: int = 3) -> Tuple[List[str], List[str]]:
    """
    Tags the given project description using the  GPTTagger instance.

    NOTE: THE TAGGER MUST BE INITIALIZED WITH VALID API KEY BEFORE CALLING THIS FUNCTION.

    Args:
        project_description (str): The project description to be tagged. Must be a non-empty string.
        tagger (GPTTagger): The tagger object used for tagging.
        number_of_tries (int, optional): The number of attempts to tag the project description. Defaults to 3.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing the tagged verticals and industries. A tuple of empty lists is returned if all attempts fail.
    """
    for i in range(number_of_tries):
        try:
            verticals, industries = await tagger.tag(project_description)
            if verticals and industries:  # Checking if lists are not empty
                return verticals, industries
        except Exception as e:  # Catching any exceptions from the tagging process
            # log the exception here
            logger.error(f'Encountered error at try {i}: {e}')

    return [], []  # Returning empty lists if all attempts fail


async def project_annotate(s, project: TrackedProject, *,
                           model_type: Literal['gpt4', 'llama'],
                           parsed_tags: list[str] | None = None,
                           parsed_details: list[str] | None = None):
    if not project.analytics:
        project.analytics = ProjectAnalytics()
        s.add(project)
        s.commit()
        s.refresh(project)

    if model_type == 'gpt4':
        tagger = GPTTagger(gpt_model='gpt-4-1106-preview', API_KEY="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")
    elif model_type == 'llama':
        tagger = GPTTagger(model_type='llama', API_KEY=LLAMA_API_KEY)
    else:
        raise ValueError(f'invalid model type: {model_type}')

    try:
        company_description = await get_company_description(project, require_all=False)
    except AnnotationError as e:
        logger.error(f'unable to annotate project {project.title}, encountered AnnotationError: {e}')
        return

    verticals, industries = await tag_with_tagger(company_description, tagger)

    logger.info(f'tagging {project.title} with description: \n{company_description}')
    logger.info(f'project {project.title} tagged with verticals: \n\t\t{verticals}, industries: \n\t\t{industries}')

    await update_tag(project, 'verticals', list(set(verticals)), source=model_type)
    await update_tag(project, 'industries', list(set(industries)), source=model_type)


    # logger.debug(f'running descriptiove annotations for project {project.title}')
    # try:
    #     await annotate_company_descriptive(s, project, company_description, parsed_tags, parsed_details)
    # except AnnotationError as e:
    #     logger.error(f'unable to annotate project {project.title}, encountered AnnotationError: {e}')


async def project_generate_description(s, project: TrackedProject):
    if not project.linkedin_profile:
        logger.critical('project has no linkedin')
        return

    linkedin_about = project.linkedin_profile.about
    if not linkedin_about:
        logger.critical('project has no linkedin.about')
        return

    logger.info(f'Generating description for {project}')

    linkedin_about  = re.sub(r'[\n\r\t]+','', linkedin_about)

    prompt = f'You are a skeptical business analyst. Based on its description, provide a description of what are the services or products of the company "{project.title}". ' \
    'DO NOT MENTIONS DATES! ONLY LIST FACTS THAT ARE AVAILABLE! DO NOT INCLUDE INFORMATION ABOUT VALUES E.G. INCLUSION, COMMUNITY ETC. YOUR DESCRIPTION MUST BE LESS THAN 90 WORDS!\n\n Company information: ' \
    f'{" ".join(linkedin_about.split())}\n\n' \
    'Provide company summary below: '

    tagger = GPTTagger(gpt_model='gpt-4-1106-preview', max_tokens=200, temperature=0.1)
    res = await tagger.chat_completion(prompt)

    # logger.critical(res)

    if not project.analytics:
        project.analytics = ProjectAnalytics()
        s.add(project)
        s.commit()
        s.refresh(project)

    project.analytics.update_detail(attr_name='description',
                                    new_value=res,
                                    data_source='gpt4')

    s.add(project)
    s.commit()

    logger.info(prompt + '\n\n' + project.description)

    publish_project(project_uuid=project.uuid, require_details_fields=False)


def load_tagging_tasks():
    tasks_categorical = read_const_file('openai/categorical_tasks.txt').read_text()
    qualitative_tags, qualitative_unstructured = read_const_file('openai/qualitative_tags.txt').read_text(), \
                                                 read_const_file('openai/qualitative_unstructered.txt').read_text()

    valid_tag_types = [re.search(r'^(\w+):', t).group(1) for t in
                       '\n'.join([tasks_categorical, qualitative_tags]).split('\n')
                       if re.search(r'^(\w+):', t)]
    valid_details_types = [re.search(r'^(\w+):', t).group(1) for t in qualitative_unstructured.split('\n')
                           if re.search(r'^(\w+):', t)]
    return valid_tag_types, valid_details_types


async def run_task_in_chunks(s, task_func, items, chunk_size = 4, **kwargs):
    """
    Runs the given task function in chunks of the given size.
    @param task_func must be a coroutine function with first argument taking an item to process
    @param items list of items to process
    @param chunk_size size of the chunks
    @param kwargs additional keyword arguments to be passed to the task function
    """
    n_chunks = math.ceil(len(items) / chunk_size)

    for i in range(0, n_chunks):
        logger.critical(f'current chunk: {i + 1} of {n_chunks}')
        logger.critical(f'items {i * chunk_size} - {(i + 1) * chunk_size}')
        await asyncio.gather(
            *[
                task_func(s, item, **kwargs) for item in items[i * chunk_size: (i + 1) * chunk_size]
            ]
        )


def get_projects_without_tag(s, tag_attr, tag_types: list[str], data_sources: list[str] | None,
                                 project_statuses: list[ProjectStatus] | None):
    match tag_attr:
        case 'tags':
            tag_class = ProjectTagsAssociation
            tag_type_attr = 'tag_type'
        case 'details':
            tag_class = DetailsEntry
            tag_type_attr = 'type'
        case _:
            raise ValueError(f'invalid tag attribute: {tag_attr}')

    tag_filter = getattr(tag_class, tag_type_attr).in_(tag_types)

    if data_sources:
        tag_filter = and_(tag_filter, getattr(tag_class, 'data_source').in_(data_sources))

    projects_query = select(TrackedProject)
    if project_statuses:
        projects_query = projects_query.where(TrackedProject.status.in_(project_statuses))

    untagged_projects = s.scalars(
                projects_query.where(
                            TrackedProject.linkedin_profile != None,
                            TrackedProject.linkedin_profile.has(LinkedinCompany.about != None),
                            ~TrackedProject.analytics.has(
                                getattr(ProjectAnalytics, tag_attr).any(tag_filter)
                            )
                ).order_by(TrackedProject.status.desc(), TrackedProject.status_changed.desc()).limit(500)
            ).all()
    return list(untagged_projects)


async def tag_projects(s):
        # tag_names, details_names = load_tagging_tasks()
        untagged_projects = get_projects_without_tag(s, 'tags', ['verticals'], ['gpt4', 'llama'],
                                                     [ProjectStatus.accepted, ProjectStatus.published, ProjectStatus.review])

        await run_task_in_chunks(s, project_annotate, untagged_projects, model_type='llama')


async def make_project_descriptions(s):
        untagged_projects = get_projects_without_tag(s, 'details', ['description'], ['gpt4', 'llama'],
                                                     [ProjectStatus.accepted, ProjectStatus.published, ProjectStatus.review])
        logger.info(f'making descriptions for {len(untagged_projects)} projects')
        await run_task_in_chunks(s, project_generate_description, untagged_projects)


if __name__ == '__main__':
    # get mode argument from the command line if present
    mode = sys.argv[1] if len(sys.argv) > 1 else None

    with Session() as s:
        if mode == 'tag':
            asyncio.run(tag_projects(s))
        elif mode == 'description':
            asyncio.run(make_project_descriptions(s))

            # asyncio.run(tag_projects(s))
        else:
            raise ValueError(f'invalid mode: {mode}')


# if __name__ == '__main__':
#     funds = [
# 'e30d8f89-e39c-457f-a884-c2005ddf1780',
# '63e98e5b-7e20-4bd2-8b8a-5a86bed8cea1',
# '065f9b6b-48aa-4cf7-9495-25328d04fc40',
# '9654bed4-4e99-42ca-b363-bb516680b1bf',
# '7f195932-7bcd-4a7a-a81e-88c812ceef68',
# 'dd8c8915-3ea6-455f-86e4-265693ed0cf3',
# '025151da-38bc-4e64-bb91-f4b94aa8a098',
# 'd75f4295-f535-4e76-8025-657f184ade2b',
# 'b1d7b81a-f93b-4128-8898-9b68eae15801',
# '781bb0d7-cfb4-44b1-99fd-84763c9d6e6d',
# '062f4aeb-5a81-46d1-bc77-02f73f0ee7b5',
# '88734361-1cc3-45c9-b98c-cf808c75460d',
# 'b6b897fd-5639-4cd5-b392-6402c67753b5',
# '53d7acf8-350b-4954-b036-17e6864f9ccc',
# '2e6f0578-5a85-4fc4-b6bb-1c448ff25bb6',
# '388fd5ad-722c-4719-b3ed-cbc747a9a119',
# '9c7f649f-b020-44f2-af81-5616064c3cce',
# '00c7882d-534a-4445-b6ab-1f8aa8204441',
# '15116ded-24e4-4636-b067-ad7e1014ad2a',
# '57355569-1c03-415f-be43-e96de4e49fec',
# '7cea180d-7560-4f83-ba49-6b29027d3458',
# 'cd8e408d-f0e0-48a4-bb92-6271c112b4a9',
#     ]

#     with Session() as s:
#         for fid in funds:
#             signals = get_unique_signals_for_fund(MongoDb, UUID(fid))

#             for uid in signals:
#                 uuid = uid['_id']
#                 p = s.scalars(select(TrackedProject).where(TrackedProject.uuid==uuid)).one()

#                 if p.status == ProjectStatus.pending:
#                     p.status = ProjectStatus.review
#                     s.add(p)
#                     s.commit()
#                 print(p)