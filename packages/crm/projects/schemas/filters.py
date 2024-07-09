from pydantic import BaseModel, root_validator


from typing import Any, Literal


class FilterConfig(BaseModel):
    filter_type: Literal['pipeline', 'gpt', 'range', 'regex', 'combined', 'tags']

    id: str
    display_name: str

    none_value_should_pass: bool = True


class RangeFilterConfig(FilterConfig):
    min_value: int | None = None
    max_value: int | None = None

    @root_validator
    def validate_any_boundary(cls, values):
        min_value = values.get('min_value')
        max_value = values.get('max_value')

        if min_value is None and max_value is None:
            raise ValueError('Either min_value or max_value must be specified')

        return values


class RegexFilterConfig(FilterConfig):
    mode: Literal['include', 'exclude']
    pattern: str


class GptFilterConfig(FilterConfig):
    prompt: str
    model: str
    required_kwargs: list[str]


class CombinedFilterConfig(FilterConfig):
    filters: list[FilterConfig]
    mode: Literal['or', 'and']

    @root_validator(pre=True)
    def get_filter_configs(cls, vals):
        configs = [f.config for f in vals.get('filters')]
        vals['filters'] = configs
        return vals

class TagsFilterConfig(FilterConfig):
    field_name: str
    tags: list[str]
    mode: Literal['exclude', 'include']
    require_all: bool


class FilterResult(BaseModel):
    filter_config: FilterConfig

    passed: bool

    input: Any
    output: Any = None
