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

from typing import Optional

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.relationship import Relationship
from gravitino.api.semantic.semantic_utils import check_no_none_elements
from gravitino.utils.precondition import Precondition


class SemanticModelDefinition:
    """The immutable, Ossie-compatible body of a Semantic Model.

    A definition has no name and no independent lifecycle, it is the value used
    by the create, load, and replace operations. Collection order is preserved so
    consumers can produce stable serialized output.
    """

    def __init__(
        self,
        datasets: list[Dataset],
        ai_context: Optional[AIContext] = None,
        relationships: Optional[list[Relationship]] = None,
        metrics: Optional[list[Metric]] = None,
        custom_extensions: Optional[list[CustomExtension]] = None,
    ):
        Precondition.check_argument(
            datasets is not None and len(datasets) > 0,
            "datasets must not be null or empty",
        )
        check_no_none_elements("datasets", datasets)
        check_no_none_elements("relationships", relationships)
        check_no_none_elements("metrics", metrics)
        check_no_none_elements("customExtensions", custom_extensions)

        self._datasets = list(datasets)
        self._ai_context = ai_context
        self._relationships = None if relationships is None else list(relationships)
        self._metrics = None if metrics is None else list(metrics)
        self._custom_extensions = (
            None if custom_extensions is None else list(custom_extensions)
        )

    def ai_context(self) -> Optional[AIContext]:
        """Returns the model-level AI context, or `None` if it is not set."""
        return self._ai_context

    def datasets(self) -> list[Dataset]:
        """Returns the datasets, which always contain at least one entry."""
        return list(self._datasets)

    def relationships(self) -> Optional[list[Relationship]]:
        """Returns the relationships, or `None` if they are not set."""
        return None if self._relationships is None else list(self._relationships)

    def metrics(self) -> Optional[list[Metric]]:
        """Returns the metrics, or `None` if they are not set."""
        return None if self._metrics is None else list(self._metrics)

    def custom_extensions(self) -> Optional[list[CustomExtension]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return (
            None if self._custom_extensions is None else list(self._custom_extensions)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemanticModelDefinition):
            return False
        return (
            self._ai_context == other.ai_context()
            and self._datasets == other.datasets()
            and self._relationships == other.relationships()
            and self._metrics == other.metrics()
            and self._custom_extensions == other.custom_extensions()
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._ai_context,
                tuple(self._datasets),
                None if self._relationships is None else tuple(self._relationships),
                None if self._metrics is None else tuple(self._metrics),
                (
                    None
                    if self._custom_extensions is None
                    else tuple(self._custom_extensions)
                ),
            )
        )

    def __repr__(self) -> str:
        return (
            f"SemanticModelDefinition(aiContext={self._ai_context!r}, "
            f"datasets={self._datasets!r}, relationships={self._relationships!r}, "
            f"metrics={self._metrics!r}, "
            f"customExtensions={self._custom_extensions!r})"
        )
