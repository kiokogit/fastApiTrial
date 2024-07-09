from typing import Any, Optional

import pydantic
from pydantic.utils import GetterDict

class AllOptional(pydantic.main.ModelMetaclass):
    def __new__(self, name, bases, namespaces, **kwargs):
        annotations = namespaces.get('__annotations__', {})
        for base in bases:
            annotations.update(base.__annotations__)
        for field in annotations:
            if not field.startswith('__'):
                annotations[field] = Optional[annotations[field]]
        namespaces['__annotations__'] = annotations
        return super().__new__(self, name, bases, namespaces, **kwargs)


class ModifiableGetter(GetterDict):
    """
    Custom GetterDict subclass allowing to modify values
    """
    def __setitem__(self, key: str, value: Any) -> Any:
        return setattr(self._obj, key, value)
