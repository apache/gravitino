# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Lossless JSON text conversion for Semantic Model DTOs."""

import json
from decimal import Decimal
from typing import Any

import simplejson
from dataclasses_json import DataClassJsonMixin


class SemanticJsonMixin(DataClassJsonMixin):
    """JSON text conversion preserving numbers in semantic AI-context properties.

    Dict conversion leaves Decimal values intact. The standard dataclasses-json
    encoder turns them into strings, so use a decimal-aware encoder at the JSON
    boundary. Decode fractional numbers as Decimal before DTO conversion to avoid
    rounding nested additional properties through binary floats.
    """

    def to_json(self, **kwargs) -> str:
        """Serialize to JSON, emitting Decimal values as exact JSON numbers.

        Keyword arguments are forwarded to simplejson.dumps, including formatting
        options such as indent and sort_keys.
        """
        return simplejson.dumps(
            _preserve_decimal_type(self.to_dict()), use_decimal=True, **kwargs
        )

    @classmethod
    def from_json(cls, s, *, parse_float=Decimal, infer_missing=False, **kwargs):
        """Deserialize JSON with lossless fractional numbers by default.

        Other decoding options are forwarded to json.loads. An explicit
        parse_float overrides the default Decimal conversion.
        """
        return cls.from_dict(
            json.loads(s, parse_float=parse_float, **kwargs),
            infer_missing=infer_missing,
        )


def _preserve_decimal_type(value: Any) -> Any:
    """Ensure Decimal values produce decimal or exponent JSON tokens.

    Decimal("1") normally emits an integer token, which decodes as int. Append a
    fractional zero only for exponent-zero decimals. Constructing from text is
    exact regardless of the active decimal context, unlike quantize or arithmetic.
    Work on a copy so the DTO's original values and scale remain unchanged.
    """
    if isinstance(value, Decimal) and value.as_tuple().exponent == 0:
        return Decimal(f"{value}.0")
    if isinstance(value, dict):
        return {key: _preserve_decimal_type(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_preserve_decimal_type(item) for item in value]
    return value
