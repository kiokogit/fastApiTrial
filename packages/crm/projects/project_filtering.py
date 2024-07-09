import asyncio
import os
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Literal, Sequence
from pprint import pprint
from uuid import UUID
from loguru import logger

from sqlalchemy.orm import Session
from sqlalchemy import select, inspect
from sqlalchemy.exc import NoResultFound

from arbm_core.private.projects import TrackedProject, ProjectAnalytics
from arbm_core.private.investors import Fund
from analysis.annotate_projects import tag_with_tagger, update_tag
from analysis.gpt_tag import GPTTagger

from api_external.openai_api import chat_completion, parse_bool_response
from projects.schemas.filters import FilterResult
from projects import FilterPreconditionException
from projects.schemas.filters import CombinedFilterConfig, FilterConfig, GptFilterConfig, RangeFilterConfig, RegexFilterConfig, TagsFilterConfig
from projects.schemas.project import ProjectData
from projects.schemas.signals import FundIdSchema
import util


DESCRIPTION_EXCLUSION_TERMS = util.read_list('excluded_descriptions.txt')
TITLE_EXCLUSION_TERMS = util.read_list('excluded_titles.txt')
IS_STARTUP_PROMPT = util.read_const_file('is_startup_prompt.txt').read_text()
THESIS_MATCH_PROMPT = util.read_const_file('thesis_match_prompt.txt').read_text()
IS_MANUFACTURING_PROMPT = util.read_const_file('manufacturing_prompt.txt').read_text()
IS_HAZMAT_PROMPT = util.read_const_file('is_hazmat_prompt.txt').read_text()

LLAMA_API_KEY = os.environ['LLAMA_API_KEY']


class ArgsPreprocessor(ABC):
    @abstractmethod
    def __call__(self, **kwargs) -> dict:
        pass


class ProjectDescriptionPreprocessor(ArgsPreprocessor):
    def __call__(self, *, project_data: ProjectData, **kwargs) -> dict:
        inputs={
            'project_data': project_data,
            'kwargs': kwargs
        }

        info_fields = ['location', 'team_size']
        startup_info = {k: v for k, v in project_data.analytics.__dict__.items()
                         if k in info_fields and v}
        startup_info = '\n'.join([f'{k}: {v}' for k, v in startup_info.items()])

        description_attrs = [getattr(project_data, attr, '')
                                for attr in [
                                            'about',
                                            'description',
                                            'category',
                                            'industry',
                                            'specialities'
                                ]
                            ]
        description_attrs = [attr[0]
                             if isinstance(attr, tuple)
                             else attr
                             for attr in description_attrs
                             ]
        # print('description_attrs')


        # for attr in description_attrs:
        #     if re.sub(r'\s+', ' ', attr).strip():
        #         print(re.sub(r'\s+', ' ', attr).strip())

        valid_descriptions = [re.sub(r'\s+', ' ', attr).strip()
                              for attr in description_attrs if attr]

        if not any(valid_descriptions):
            raise FilterPreconditionException('no valid description attributes found',
                                              inputs=inputs)

        startup_data = startup_info + '\n' + '\n'.join(valid_descriptions)

        if len(startup_data) < 30:
            raise FilterPreconditionException('startup description is too short',
                                                inputs=inputs)

        return {
            'startup_data': startup_data,
            'company_name': project_data.title,
        }


class ProjectTagsPreprocessor(ArgsPreprocessor):
    def __call__(self, *, db, project_data: ProjectData, startup_data: str, **kwargs) -> dict:
        # try to find project in the db
        # if found - check if gpt4 tags exist
        # if found and tags exist - return tags
        website = project_data.website
        project = db.scalars(select(TrackedProject).where(TrackedProject.website == website)).one_or_none()
        if project and project.analytics:
            tags = [t.tag_name for t in project.analytics.get_attr('industries', 'tag') if t.data_source == 'gpt4']

            if tags:
                return {'tags': tags}

        tagger = GPTTagger(model_type='llama', API_KEY=LLAMA_API_KEY)

        verticals, industries = asyncio.run(tag_with_tagger(startup_data, tagger))
        # if not found or tags don't exist, run gpt4 tagger
            # raise preconditon exception if times out

        if project:
            db.add(project)
            # function automatically commits values to db
            asyncio.run(update_tag(project, 'verticals', list(set(verticals)), source='gpt4'))
            asyncio.run(update_tag(project, 'industries', list(set(industries)), source='gpt4'))

        return {
            'industries': industries
        }


class FundPreprocessor(ArgsPreprocessor):
    def __call__(self, *, db, signal: FundIdSchema, **kwargs) -> dict:
        inputs={
            'signal': signal,
            'kwargs': kwargs
        }

        try:
            fund = db.scalars(select(Fund).where(Fund.id == signal.id)).one()
        except NoResultFound:
            raise FilterPreconditionException(f'fund with id {signal.id} not found',
                                              inputs=inputs)

        inputs.update(fund_data=fund.to_dict())
        if not fund.thesis:
            raise FilterPreconditionException(f'fund with id {signal.id} ({fund.name}) has no thesis',
                                              inputs=inputs)

        return {
            'fund_id': fund.id,
            'fund_name': fund.name,
            'thesis': fund.thesis,
            # 'startup_data': startup_data,
            # 'company_name': project_data.title,
        }


class ProjectFilter(ABC):
    def __init__(self,
                 id: str,
                 display_name: str,
                 none_value_should_pass: bool = True, **kwargs) -> None:
        self.id = id
        self.display_name = display_name
        self.none_value_should_pass = none_value_should_pass

    @property
    def config(self):
        match self.__class__.__name__:
            case 'GptFilter':
                return GptFilterConfig(**self.__dict__)
            case 'RangeFilter':
                return RangeFilterConfig(**self.__dict__)
            case 'RegexFilter':
                return RegexFilterConfig(**self.__dict__)
            case 'CombinedFilter':
                return CombinedFilterConfig(**self.__dict__)
            case 'TagsFilter':
                return TagsFilterConfig(**self.__dict__)
            case _:
                raise ValueError(f'unknown filter type: {self.__class__.__name__}')

    @abstractmethod
    def __call__(self, project_data: ProjectData, **kwargs) -> FilterResult:
        pass


class GptFilter(ProjectFilter):
    def __init__(self, prompt: str, required_kwargs: list[str], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.filter_type = 'gpt'
        self.prompt = prompt
        self.model = 'gpt-4-1106-preview'
        self.required_kwargs = required_kwargs

    def __call__(self, project_data: ProjectData, **kwargs) -> FilterResult:
        prompt_kwargs = kwargs.copy()
        prompt_kwargs.update(project_data.dict())

        if not all(key in prompt_kwargs for key in self.required_kwargs):
            raise ValueError('filter is missing kwargs required for prompt formatting.'
                             '\nmake sure they are either contained in project data or '
                             f'provided as **kwargs.\nmissing args: ' + str([key for key in self.required_kwargs
                                                                      if not key in prompt_kwargs])
                                                                      + '\nall args: ' + str(prompt_kwargs))

        prompt = self.prompt.format(**prompt_kwargs)

        passed = asyncio.run(
            chat_completion(
                prompt,
                response_validator=parse_bool_response,
                model=self.model
            )
        )

        # pprint(prompt)
        # print(passed)

        if not isinstance(passed, bool):
            raise ValueError('response_validator did not return a bool')

        return FilterResult(filter_config=self.config, passed=passed, input=prompt)


class RangeFilter(ProjectFilter):
    def __init__(self,
                 field_getter: Callable[[ProjectData], Any],
                 min_value: int | None = None,
                 max_value: int | None = None,
                 *args, **kwargs) -> None:
        if min_value is None and max_value is None:
            raise ValueError('min_value and max_value cannot both be None')

        super().__init__(*args, **kwargs)

        self.min_value = min_value
        self.max_value = max_value
        self.field_getter = field_getter
        self.filter_type = 'range'

    def __call__(self, project_data: ProjectData, **kwargs) -> FilterResult:
        if (value := self.field_getter(project_data)) is None:
            return FilterResult(filter_config=self.config, passed=self.none_value_should_pass, input=None)

        if self.min_value is not None and value < self.min_value:
            return FilterResult(filter_config=self.config, passed=False, input=value)

        if self.max_value is not None and value > self.max_value:
            return FilterResult(filter_config=self.config, passed=False, input=value)

        return FilterResult(filter_config=self.config, passed=True, input=value)


class RegexFilter(ProjectFilter):
    def __init__(self, pattern: str, field_getter: Callable[[ProjectData], Any], mode: Literal['include', 'exclude'], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.pattern = pattern
        self.field_getter = field_getter
        self.filter_type = 'regex'
        self.mode = mode

    def __call__(self, project_data: ProjectData, **kwargs) -> FilterResult:
        if (text := self.field_getter(project_data)) is None:
            return FilterResult(filter_config=self.config,
                                passed=self.none_value_should_pass, input=None)

        if isinstance(text, tuple):
            text = text[0]

        match = re.search(self.pattern, text, re.IGNORECASE)

        if self.mode == 'include':
            # return false if no included words are found
            if match is None:
                return FilterResult(filter_config=self.config,
                                    passed=False, input=text)
        elif self.mode == 'exclude':
            # return false if any excluded words are found
            if match is not None:
                return FilterResult(filter_config=self.config,
                                    passed=False, input=text, output=match.groups())

        return FilterResult(filter_config=self.config,
                            passed=True, input=text)


class TagsFilter(ProjectFilter):
    def __init__(self,
                #  field_getter: Callable[[ProjectData], Any],
                 field_name: str,
                 tags: set[str],
                 mode: Literal['include', 'exclude'] = 'include',
                 require_all: bool = False,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.tags = tags
        self.mode = mode
        self.require_all = require_all

        # self.field_getter = field_getter
        self.field_name = field_name
        self.filter_type = 'tags'

    def __call__(self, project_data: ProjectData, **kwargs) -> FilterResult:
        tags = getattr(project_data, self.field_name, None) or kwargs.get(self.field_name)

        if tags == [] or tags is None:
            return FilterResult(filter_config=self.config,
                                passed=self.none_value_should_pass, input=None)

        common_tags = set(tags).intersection(self.tags)
        if self.require_all:
            has_common = len(common_tags) == len(self.tags)
        else:
            has_common = any(common_tags)

        passed = has_common if self.mode == 'include' else not has_common

        return FilterResult(filter_config=self.config,
                            passed=passed, input=tags)


class CombinedFilter(ProjectFilter):
    def __init__(self, filters: Sequence[ProjectFilter], mode: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if mode not in ('and', 'or'):
            raise ValueError('mode must be either "and" or "or"')

        self.mode = mode
        self.filters = filters
        self.filter_type = 'combined'

    def __call__(self, project_data: ProjectData, **kwargs) -> FilterResult:
        passed = False
        results = []

        for filter_ in self.filters:
            filter_result: FilterResult = filter_(project_data)

            results.append(filter_result)

            if self.mode == 'or' and filter_result.passed:
                passed = True
                break

        values = [result.passed for result in results]

        if self.mode == 'and' and all(values):
            passed = True

        return FilterResult(filter_config=self.config, passed=passed, input=None, output=results)


# todo: filter sets can be dynamically loaded

PROJECT_PREPROCESSORS = [
    # preprocess args for thesis matching
    ProjectDescriptionPreprocessor(),
]

EXCLUSION_FILTERS = [
        # title
        RegexFilter(
            id='title',
            display_name='Title',
            field_getter=lambda p: p.title,
            mode='exclude',
            pattern=rf'\b({ "|".join(TITLE_EXCLUSION_TERMS) })\b'
        ),
        # description
        # description_filters = RegexFilter(
        #     id='description',
        #     display_name='Description',
        #     field_getter=lambda p: p.description,
        #     pattern=rf'\b({ "|".join(DESCRIPTION_EXCLUSION_TERMS) })\b'
        # )
]


FILTER_IS_STARTUP = PROJECT_PREPROCESSORS + [
    # exclude non-startups
    GptFilter(
        id='exclude_non_startups',
        display_name='Exclude non-startups',
        prompt=IS_STARTUP_PROMPT,
        required_kwargs=re.findall(r'\{([A-Za-z0-9]+)\}', IS_STARTUP_PROMPT)
    ),
]


# filter to determine if the signal matches the fund's thesis
FILTER_VALID_SIGNAL = PROJECT_PREPROCESSORS + [
    FundPreprocessor(),

    # thesis matching
    GptFilter(
       id='thesis_matching',
       display_name='Thesis matching',
       prompt=THESIS_MATCH_PROMPT,
       required_kwargs=re.findall(r'\{([A-Za-z0-9]+)\}', THESIS_MATCH_PROMPT)
    )
]

# FILTER_B2B_SIGNAL = [
#     # preprocess args for thesis matching
#     ProjectPreprocessor(),
#     FundPreprocessor(),

#     # thesis matching
#     GptFilter(
#        id='b2b_thesis_matching',
#        required_kwargs=re.findall(r'\{([A-Za-z0-9]+)\}', B2B_THESIS_MATCH_PROMPT)
#     )
# ]


# filters to determine if we should track project at all,
# i.e. cut off clearly bad projects (e.g. with 1 team member)
FILTER_STARTUP_DATA = EXCLUSION_FILTERS + [
    # hard filters
    # CombinedFilter(
    #     id='hard_filters',
    #     mode='and',
    #     filters=(

            # team size
            RangeFilter(
                id='team_size',
                display_name='Team size',
                field_getter=lambda p: p.analytics.team_size,
                min_value=2,
                max_value=100
            ),
            # funding
            # RangeFilter(
            #     id='funding',
            #     display_name='Funding',
            #     field_getter=lambda p: p.analytics.funding or p.analytics.last_round_amount,
            #     max_value=100000
            # ),
            # founded date
            RangeFilter(
                id='founded_year',
                display_name='Founded year',
                field_getter=lambda p: p.analytics.founded,
                min_value=2016
            ),
        # )
    # )
]


FILTER_B2B_PROJECTS = PROJECT_PREPROCESSORS + EXCLUSION_FILTERS + [
    RangeFilter(
        id='b2b_team_size',
        display_name='Team size (B2B)',
        field_getter=lambda p: p.analytics.team_size,
        min_value=5,
        max_value=5000
    ),

    RegexFilter(
        id='is_location_usa',
        display_name='Is location USA',
        field_getter=lambda p: p.analytics.location,
        mode='include',
        pattern='USA?|united states',
        none_value_should_pass=False
    ),

    GptFilter(
        id='is_manufacturing',
        display_name='Is manufacturing',
        prompt=IS_MANUFACTURING_PROMPT,
        required_kwargs=re.findall(r'\{([A-Za-z0-9]+)\}', IS_MANUFACTURING_PROMPT)
    ),

    GptFilter(
        id='is_hazmat',
        display_name='Is HAZMAT',
        prompt=IS_HAZMAT_PROMPT,
        required_kwargs=re.findall(r'\{([A-Za-z0-9]+)\}', IS_MANUFACTURING_PROMPT)
    ),

    # ProjectTagsPreprocessor(),

    # TagsFilter(
    #     id='b2b_tags',
    #     display_name='Industries (B2B)',

    #     none_value_should_pass=False,

    #     field_name='industries',
    #     tags={'TRANSPORTATION'}
    #     # field_getter=lambda p: p.analytics.industries,
    # )
]


def validate_filter_config(filters: Sequence[ProjectFilter], depth: int = 3):
    """
    Check the maximum filter depth is not exceeded
    """
    for filter in filters:
        if isinstance(filter, CombinedFilter):
            if depth == 0:
                raise ValueError('maximum filter depth exceeded')

            validate_filter_config(filter.filters, depth - 1)


def apply_filters(db: Session,
                  *,
                  id: str,
                  name: str,
                  filters: Sequence[ProjectFilter],
                  **kwargs):
    validate_filter_config(filters=filters)

    kwargs = (kwargs or {})
    kwargs.update(db=db)

    steps = []
    passed = False

    for step_ in filters:
        if isinstance(step_, ArgsPreprocessor):
            # print(f'Applying {type(step_).__name__}')
            new_kwargs = step_(**kwargs)
            kwargs.update(new_kwargs)
            continue

        # print(f'Applying filter {step_.display_name}')
        # pprint(kwargs)
        # if 'required_kwargs' in step_.__dict__:

        result = step_(**kwargs)
        steps.append(result)

        # stop filtering on the first failed filter
        passed = result.passed
        if not passed:
            break

    # ensure FilterResult is serializable
    if 'db' in kwargs:
        del kwargs['db']

    return FilterResult(
        filter_config=FilterConfig(
            id=id,
            display_name=name,
            filter_type='pipeline',
            none_value_should_pass=False,
        ),
        input=kwargs,
        output=steps,
        passed=passed,
    )


def filter_is_startup(db: Session,
                      project_data: ProjectData,
                   ) -> FilterResult:
    return apply_filters(db,
                         id='filter_is_startup',
                         name='Filter is startup',
                         filters=FILTER_IS_STARTUP,
                         project_data=project_data,
                         )


def filter_signal(db: Session,
                   signal: FundIdSchema,
                   company: ProjectData,
                ) -> FilterResult:
    return apply_filters(db,
                         id='filter_signal_valid',
                         name='Filter signal valid',
                         filters=FILTER_VALID_SIGNAL,
                         signal=signal,
                         project_data=company,
                         )


def filter_b2b_signal(db: Session,
                signal: FundIdSchema,
                company: ProjectData,
                ) -> FilterResult:
     return apply_filters(db,
                             id='filter_b2b_signal_valid',
                             name='Filter B2B signal valid',
                             filters=FILTER_B2B_PROJECTS,
                             project_data=company,
                             signal=signal,
                             )


def filter_company(db: Session,
                   company: ProjectData,
                ) -> FilterResult:
    return apply_filters(db,
                         id='filter_company_data',
                         name='Filter company data',
                         filters=FILTER_STARTUP_DATA,
                         project_data=company,
                         )


def filter_b2b_company(db: Session,
                   company: ProjectData,
                ) -> FilterResult:
    return apply_filters(db,
                         id='filter_competitors_company_data',
                         name='Filter competitors company data',
                         filters=FILTER_B2B_PROJECTS,
                         project_data=company,
                         )
