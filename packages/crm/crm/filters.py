from abc import ABC
import datetime
import enum
import re
from typing import Any, Callable, Generic, Literal, Type, TypeVar
from pydantic import BaseModel, root_validator, validator

from sqlalchemy import and_, func, or_, select, nullslast, desc

from loguru import logger

from arbm_core.private.projects import InvestmentStage, LinkedinCompany, \
    ProjectAnalytics, ProjectStatus, ProjectTag, ProjectTagsAssociation, TrackedProject
from arbm_core.private.twitter import TwitterProject

from crm.dependencies import QueryParams



def prepare_query(query: str) -> str:
    clean_query = re.sub(r'\s+', '%', query.strip().lower())
    return f'%{clean_query}%'


T = TypeVar('T')


class FilterFormType(str, enum.Enum):
    text_field = 'string'


class BaseFilterSchema(ABC, BaseModel):
    identifier: str


class ValueFilterSchema(BaseFilterSchema):
    value: str


class BoolFilterSchema(BaseFilterSchema):
    value: bool


class ChoicesFilterSchema(BaseFilterSchema, Generic[T]):
    allow_and: bool = True
    mode: Literal['OR', 'AND'] = 'OR'
    choices: list[T]

    @validator('mode')
    def prevent_and_if_not_allowed(cls, v, values):
        if v == 'AND' and not values['allow_and']:
            raise ValueError('"AND" mode is not allowed for this filter')
        return v

    class Config:
        fields = {'value': {'exclude': True}}


class MinMaxFilterSchema(BaseFilterSchema):
    min: Any | None
    max: Any | None

    @root_validator
    def validate_any(cls, values):
        if not 'min' in values and not 'max' in values:
            raise ValueError('Either min or max must be provided')
        return values

    class Config:
        fields = {'value': {'exclude': True}}


class FilterModel:
    id: str

    def __init__(self,
                 id: str,
                 type: FilterFormType,

                 obj: Type[T],
                 filter_expr: Callable,

                 filter_schema: Type[BaseFilterSchema],

                 preprocess_fn: Callable | None = None,
                 ) -> None:
        self.id = id

        self.obj = obj
        self.preprocess_fn = preprocess_fn  # prepare incoming data
        self.filter_expr = filter_expr  # used to construct orm filter

        # used to validate incoming data
        self.filter_schema = filter_schema


    def apply(self, value):
        if self.preprocess_fn:
            value = self.preprocess_fn(value)

        # return self.filter_expr(value)


PROJECT_SEARCH_FORM = [
    FilterModel(
        'id',
        FilterFormType.text_field,
        obj=TrackedProject,
        filter_expr=lambda x: TrackedProject.id == x,
        filter_schema=BaseFilterSchema
    ),

    FilterModel(
        'title',
        FilterFormType.text_field,
        obj=TrackedProject,
        preprocess_fn=prepare_query,
        filter_expr=lambda x: TrackedProject.title.ilike(x),
        filter_schema=BaseFilterSchema
    ),

    FilterModel(
        'description',
        FilterFormType.text_field,
        obj=TrackedProject,
        preprocess_fn=prepare_query,
        filter_expr=lambda x: TrackedProject.linkedin_profile.has(
                LinkedinCompany.about.ilike(x)
        ),
        filter_schema=BaseFilterSchema
    ),
]


PROJECT_FILTERS = [
    {
        'filters': {
            'project_type': {
                'type': 'multiple_choice',
                'allow'
                'label': 'Project type',
                'values': ['is_startup', 'is_b2b'],
                'filter': lambda x: getattr(TrackedProject, x, None) == True,
                'default': ['is_startup']
            },
            'id': {
                'type': 'string',
                'label': 'Project id',
                'filter_preprocess': lambda project_id_str: int(project_id_str) if project_id_str.isnumeric() else None,
                'filter': lambda project_id: TrackedProject.id == project_id,
            },
            'title': {
                'type': 'string',
                'label': 'Project title',
                'filter_preprocess': prepare_query,
                'filter': lambda query: TrackedProject.title.ilike(query),
            },
            'description': {
                'type': 'string',
                'label': 'Project description',
                'filter_preprocess': prepare_query,
                'filter': lambda query: TrackedProject.linkedin_profile.has(
                    LinkedinCompany.about.ilike(query)
                ),
            },
            'last_parsed_date': {
                'label': 'Parsed date',
                'type': 'date',
                'min': "2022-01-01",
                'max': datetime.date.today().strftime("%Y-%m-%d"),
                'filter_min': lambda x:
                            func.greatest(
                                    TrackedProject.status_changed,
                                    TrackedProject.discovered_date,
                                    LinkedinCompany.last_parsed
                            ) >= datetime.datetime.strptime(x, "%Y-%m-%d").astimezone(pytz.UTC),
                'filter_max': lambda x:
                            func.greatest(
                                    TrackedProject.status_changed,
                                    TrackedProject.discovered_date,
                                    LinkedinCompany.last_parsed
                            ) <= datetime.datetime.strptime(x, "%Y-%m-%d").astimezone(pytz.UTC),
            },
            'status': {
                'type': 'multiple_choice',
                'label': 'Project status',
                'values': [s.name for s in ProjectStatus],
                'filter': lambda x: TrackedProject.status == x,
                'default': [ProjectStatus.pending.name, ProjectStatus.discovered.name]
            },
            'verticals': {
                'type': 'multiple_choice',
                'label': 'Verticals',
                'filter': lambda x: TrackedProject.analytics.has(ProjectAnalytics.tags.any(
                    ProjectTagsAssociation.tag_name == x
                )),
                'values_func': lambda s: [n for n in s.scalars(select(ProjectTag.name)
                                                                .filter_by(type='verticals')).all()],
            },
            'team_size': {
                'type': 'int',
                'label': 'Team size',
                'min': 0,
                'max': 1000000,
                'filter_min': lambda x: TrackedProject.analytics.has(ProjectAnalytics.team_size >= x),
                'filter_max': lambda x: TrackedProject.analytics.has(ProjectAnalytics.team_size <= x),
            },
        }
    },
    {
        'section': 'finance',
        'title': 'Financial data',
        'filters': {
            'stage': {
                'type': 'enum',
                'label': 'Project stage',
                'values': [s for s in InvestmentStage],
                'filter': lambda x: TrackedProject.analytics.has(ProjectAnalytics.stage == InvestmentStage(x)),
            },
            'funding': {
                'type': 'int',
                'formatting': 'thousands',
                'min': 0,
                'filter_min': lambda x: TrackedProject.analytics.has(ProjectAnalytics.funding >= x),
                'filter_max': lambda x: TrackedProject.analytics.has(ProjectAnalytics.funding <= x),
            },
            'last_round_date': {
                'type': 'date',
                'label': 'Investment date',
                'min': datetime.date.today().strftime("1990-01-01"),
                'max': datetime.date.today().strftime("%Y-%m-%d"),
                'filter_min': lambda x: TrackedProject.analytics.has(ProjectAnalytics.last_round > x),
                'filter_max': lambda x: TrackedProject.analytics.has(ProjectAnalytics.last_round < x),
            },
            'last_round_qty': {
                'type': 'int',
                'formatting': 'thousands',
                'label': 'Last round quantity',
                'min': 0,
                'filter_min': lambda x: TrackedProject.analytics.has(ProjectAnalytics.last_round_amount >= x),
                'filter_max': lambda x: TrackedProject.analytics.has(ProjectAnalytics.last_round_amount <= x),
            },
            'recent_investment': {
                'type': 'bool',
                'label': 'Investment acquired recently',
                'filter': lambda x: TrackedProject.analytics.has(ProjectAnalytics.recent_investment == x),
            },
            'previous_exit': {
                'type': 'bool',
                'label': 'Had previous exit',
                'filter': lambda x: TrackedProject.analytics.has(ProjectAnalytics.previous_exit == x),
            },
            # 'founded': {'type': 'year'},
        }
    }
]


class ProjectFiltersSchema(BaseModel):
    filters: list[BoolFilterSchema | ValueFilterSchema | ChoicesFilterSchema | MinMaxFilterSchema]


def make_filters(filters: list[dict], #list[BaseFilterSchema],
                 filters_config: dict):
    orm_filters = []

    all_filters = {fid: filter_ for section in filters_config
                        for fid, filter_ in section.get('filters', {}).items()
                  }

    for f in filters.get('filters', {}):
        if (fid := f['identifier']) not in all_filters.keys():
            raise ValueError(f'could not find config for filter {fid}')
        config = all_filters[fid]

        # logger.error(f)
        # logger.error(config)

        filter_type = config['type']

        if filter_type in ('int', 'date'):
            # case RangeFilterSchema():
            min, max = f.get('min'), f.get('max')
            if min:
                orm_filters.append(config['filter_min'](min))
            if max:
                orm_filters.append(config['filter_max'](max))

        elif filter_type == 'multiple_choice':
            # case ChoicesFilterSchema():
                # logger.error(f)
                chosen_options = [config['filter'](c) for c in f['choices']]

                logic = or_ if f.get('mode') == 'OR' else and_
                orm_filters.append(logic(*chosen_options))
        # elif filter_type == 'bool':
        #     # case BooleanFilterSchema():
        #     orm_filters.append(config['filter'](f.value))
        else:
            # case SelectFilterSchema():
            val = f['value']

            if preprocess_fn := config.get('filter_preprocess'):
                val = preprocess_fn(val)

            orm_filters.append(config['filter'](val))

    return orm_filters


def filter_objects(db, orm_obj: T, q, filters: dict) -> tuple[int, list[T]]:
    max_results = q.limit
    offset = q.offset

    join_to = []
    order_expr = None

    match orm_obj:
        case TrackedProject:
            join_to = [LinkedinCompany, TwitterProject]
            order_expr = [nullslast(desc(func.greatest(
                            TrackedProject.status_changed,
                            TrackedProject.discovered_date
                            # LinkedinCompany.last_parsed
                        )))]

            orm_filters = make_filters(filters, PROJECT_FILTERS)

    base_query = select(orm_obj).filter(*orm_filters)

    joined_query = base_query
    for join_obj in join_to:
        joined_query = joined_query.join(join_obj, isouter=True)

    full_query = joined_query.order_by(*order_expr)\
            .offset(offset).limit(max_results)

    results = db.scalars(full_query).all()
    results_count = db.execute(select(func.count()).select_from(base_query.subquery())).scalar_one()

    return results_count, results
