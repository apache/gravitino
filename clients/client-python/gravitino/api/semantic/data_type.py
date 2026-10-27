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

from enum import Enum


class DataType(Enum):
    """The logical type vocabulary used by Semantic Model fields and metrics.

    This vocabulary is derived from the Apache Ossie Core specification and is
    deliberately independent of the Gravitino relational type system. Gravitino
    does not infer or convert these values from source column types.

    The enum value is the exact Ossie wire value, for example ``DataType.DECIMAL``
    is serialized as ``"Decimal"``.
    """

    STRING = "String"
    INTEGER = "Integer"
    DECIMAL = "Decimal"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    DATE = "Date"
    TIME = "Time"
    DATE_TIME = "DateTime"
    DATE_TIME_TZ = "DateTimeTz"
    OPAQUE = "Opaque"
