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


class ConceptType:
    """The closed set of concept types an ontology concept can have.

    The set is closed by the Apache Ossie ontology specification, which allows
    exactly these two values.
    """

    ENTITY_TYPE: ClassVar[str] = "ENTITY_TYPE"
    """A concept whose instances are identifiable objects, such as an account."""

    VALUE_TYPE: ClassVar[str] = "VALUE_TYPE"
    """A concept that represents a value rather than an identifiable object."""

    ALL: ClassVar[tuple[str, ...]] = (ENTITY_TYPE, VALUE_TYPE)
    """All the concept type values, used to validate user input."""
