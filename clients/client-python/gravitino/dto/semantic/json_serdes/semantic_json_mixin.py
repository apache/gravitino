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
        return simplejson.dumps(self.to_dict(), use_decimal=True, **kwargs)

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
