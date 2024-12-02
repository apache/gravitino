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
from decimal import Decimal as PyDecimal, ROUND_HALF_UP
from typing import Optional


class Decimal:
    """
    Used to represent a Types.DecimalType value in Apache Gravitino.
    """

    _value: PyDecimal
    _precision: int
    _scale: int

    def __init__(self, value: PyDecimal, precision: int, scale: int):
        if precision is not None and scale is not None:
            self.check_precision_scale(precision, scale)
            assert precision >= len(value.as_tuple().digits), (
                f"Precision of value cannot be greater than precision of decimal: "
                f"{value.as_tuple().digits} > {precision}"
            )
            self._value = value.quantize(
                PyDecimal((0, (1,), -scale)), rounding=ROUND_HALF_UP
            )
            self._precision = precision
            self._scale = scale
        else:
            self._value = value
            self._precision = max(
                len(value.as_tuple().digits), -value.as_tuple().exponent
            )
            self._scale = -value.as_tuple().exponent

    @staticmethod
    def of(value, precision: Optional[int] = None, scale: Optional[int] = None):
        if isinstance(value, str):
            _value = PyDecimal(value)
        return Decimal(value, precision, scale)

    @staticmethod
    def check_precision_scale(precision: int, scale: int):
        # Implement the check logic here
        pass

    def value(self) -> PyDecimal:
        return self._value

    def precision(self) -> int:
        return self._precision

    def scale(self) -> int:
        return self._scale

    def __str__(self) -> str:
        return str(self._value)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Decimal):
            return False
        return self._scale == other.scale() and self._value == other.value()

    def __hash__(self) -> int:
        return hash((self._value, self._precision, self._scale))
