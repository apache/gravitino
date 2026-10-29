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
from __future__ import annotations

from typing import ClassVar


class Multiplicity:
    """The cardinality of an ontology relationship.

    The set is closed by the Apache Ossie ontology specification. An omitted
    multiplicity means the relationship is unconstrained.
    """

    MANY_TO_ONE: ClassVar[str] = "MANY_TO_ONE"
    """Many instances of the owning concept refer to one related instance."""

    ONE_TO_ONE: ClassVar[str] = "ONE_TO_ONE"
    """Each instance of the owning concept refers to exactly one related instance."""

    ALL: ClassVar[tuple[str, ...]] = (MANY_TO_ONE, ONE_TO_ONE)
    """All the multiplicity values, used to validate user input."""
