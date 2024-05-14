"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Mapping, Sequence, Union

# https://github.com/python/typing/issues/182#issuecomment-1320974824
JSONType = Union[
    Mapping[str, "JSONType"], Sequence["JSONType"], str, int, float, bool, None
]
