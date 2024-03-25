"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from typing import Mapping, Sequence, Union, TypeAlias

# https://github.com/python/typing/issues/182#issuecomment-1320974824
JSON_ro: TypeAlias = Union[Mapping[str, "JSON_ro"], Sequence["JSON_ro"], str, int,  float, bool, None]
