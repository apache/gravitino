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

from gravitino.api.semantic.metric import Metric
from gravitino.namespace import Namespace
from gravitino.utils.precondition import Precondition


class SemanticModelMetric:
    """A discovered metric with its containing Semantic Model's namespace.

    Catalog-scoped discovery returns a namespace containing the schema and model
    names. The metric's name remains part of its complete :class:`Metric` value.
    """

    def __init__(self, namespace: Namespace, metric: Metric):
        Precondition.check_argument(namespace is not None, "namespace must not be null")
        Precondition.check_argument(metric is not None, "metric must not be null")
        self._namespace = Namespace.of(*namespace.levels())
        self._metric = metric

    def namespace(self) -> Namespace:
        """Return a copy of the namespace ending with the containing model's name."""
        return Namespace.of(*self._namespace.levels())

    def metric(self) -> Metric:
        """Return the complete metric definition."""
        return self._metric

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemanticModelMetric):
            return False
        return self._namespace == other._namespace and self._metric == other._metric

    def __hash__(self) -> int:
        return hash((self._namespace, self._metric))

    def __repr__(self) -> str:
        return (
            f"SemanticModelMetric(namespace={self._namespace.levels()!r}, "
            f"metric={self._metric!r})"
        )
