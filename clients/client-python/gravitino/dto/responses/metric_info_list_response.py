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

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_model_metric_dto import SemanticModelMetricDTO
from gravitino.utils.precondition import Precondition


@dataclass
class MetricInfoListResponse(SemanticJsonMixin, BaseResponse):
    """Response containing complete metrics discovered in a schema."""

    _metrics: list[SemanticModelMetricDTO] = field(
        metadata=config(field_name="metrics")
    )

    def metrics(self) -> list[SemanticModelMetricDTO]:
        """Return the metric discovery results."""
        return self._metrics

    def validate(self):
        """Validate the response code and metric result list."""
        super().validate()
        Precondition.check_argument(
            self._metrics is not None, "metrics must not be null"
        )
        Precondition.check_argument(
            all(metric is not None for metric in self._metrics),
            "metrics must not contain null",
        )
