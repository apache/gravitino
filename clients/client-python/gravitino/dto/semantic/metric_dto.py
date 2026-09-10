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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.metric import Metric
from gravitino.dto.semantic.ai_context_dto import AIContextDTO
from gravitino.dto.semantic.custom_extension_dto import CustomExtensionDTO
from gravitino.dto.semantic.expression_dto import ExpressionDTO
from gravitino.dto.semantic.json_serdes.ai_context_serdes import AIContextSerdes
from gravitino.dto.semantic.json_serdes.data_type_serdes import DataTypeSerdes
from gravitino.dto.semantic.semantic_dto_utils import convert_list, is_none
from gravitino.utils.precondition import Precondition


@dataclass
class MetricDTO(DataClassJsonMixin):
    """Represents a Semantic Model metric DTO."""

    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )
    _expression: Optional[ExpressionDTO] = field(
        default=None, metadata=config(field_name="expression", exclude=is_none)
    )
    _description: Optional[str] = field(
        default=None,
        metadata=config(field_name="description", exclude=is_none),
    )
    _datatype: Optional[DataType] = field(
        default=None,
        metadata=config(
            field_name="datatype",
            encoder=DataTypeSerdes.serialize,
            decoder=DataTypeSerdes.deserialize,
            exclude=is_none,
        ),
    )
    _ai_context: Optional[AIContextDTO] = field(
        default=None,
        metadata=config(
            field_name="ai_context",
            encoder=AIContextSerdes.serialize,
            decoder=AIContextSerdes.deserialize,
            exclude=is_none,
        ),
    )
    _custom_extensions: Optional[list[CustomExtensionDTO]] = field(
        default=None,
        metadata=config(field_name="custom_extensions", exclude=is_none),
    )

    def name(self) -> Optional[str]:
        """Returns the metric name."""
        return self._name

    def expression(self) -> Optional[ExpressionDTO]:
        """Returns the expression that computes the metric."""
        return self._expression

    def description(self) -> Optional[str]:
        """Returns the metric description, or `None` if it is not set."""
        return self._description

    def datatype(self) -> Optional[DataType]:
        """Returns the logical data type, or `None` if it is not set."""
        return self._datatype

    def ai_context(self) -> Optional[AIContextDTO]:
        """Returns the AI context, or `None` if it is not set."""
        return self._ai_context

    def custom_extensions(self) -> Optional[list[CustomExtensionDTO]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return self._custom_extensions

    @staticmethod
    def from_metric(metric: Metric) -> "MetricDTO":
        """Convert a metric to its DTO."""
        ai_context = metric.ai_context()
        return MetricDTO(
            _name=metric.name(),
            _expression=ExpressionDTO.from_expression(metric.expression()),
            _description=metric.description(),
            _datatype=metric.datatype(),
            _ai_context=(
                None if ai_context is None else AIContextDTO.from_ai_context(ai_context)
            ),
            _custom_extensions=convert_list(
                metric.custom_extensions(), CustomExtensionDTO.from_custom_extension
            ),
        )

    def to_metric(self) -> Metric:
        """Convert this DTO to a metric."""
        Precondition.check_argument(
            self._expression is not None, "expression must not be null"
        )
        return Metric(
            name=self._name,
            expression=self._expression.to_expression(),
            description=self._description,
            datatype=self._datatype,
            ai_context=(
                None if self._ai_context is None else self._ai_context.to_ai_context()
            ),
            custom_extensions=convert_list(
                self._custom_extensions, CustomExtensionDTO.to_custom_extension
            ),
        )
