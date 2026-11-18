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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.api.semantic.metric import Metric
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.metric_dto import MetricDTO
from gravitino.namespace import Namespace
from gravitino.utils.precondition import Precondition


@dataclass
class SemanticModelMetricDTO(SemanticJsonMixin):
    """A metric's containing namespace and complete definition from discovery."""

    _namespace: Optional[list[str]] = field(
        default=None, metadata=config(field_name="namespace")
    )
    _metric: Optional[MetricDTO] = field(
        default=None, metadata=config(field_name="metric")
    )

    def namespace(self) -> Namespace:
        """Return the namespace ending with the containing model's name."""
        Precondition.check_argument(
            self._namespace is not None, "namespace must not be null"
        )
        return Namespace.of(*self._namespace)

    def metric(self) -> Metric:
        """Decode the complete metric definition."""
        Precondition.check_argument(self._metric is not None, "metric must not be null")
        return self._metric.to_metric()
