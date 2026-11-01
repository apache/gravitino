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

from gravitino.name_identifier import NameIdentifier
from gravitino.utils.precondition import Precondition


class PlanSource:
    """A dataset required by a `SemanticQueryPlan` and its physical source."""

    def __init__(self, dataset: str, source: NameIdentifier):
        Precondition.check_string_not_empty(
            dataset, "dataset must not be null or blank"
        )
        Precondition.check_argument(source is not None, "source must not be null")
        self._dataset = dataset
        self._source = source

    def dataset(self) -> str:
        """Returns the dataset name declared by the Semantic Model."""
        return self._dataset

    def source(self) -> NameIdentifier:
        """Returns the physical source identifier."""
        return self._source

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PlanSource):
            return False
        return self._dataset == other.dataset() and self._source == other.source()

    def __hash__(self) -> int:
        return hash((self._dataset, self._source))

    def __repr__(self) -> str:
        return f"PlanSource(dataset={self._dataset!r}, source={self._source!r})"
