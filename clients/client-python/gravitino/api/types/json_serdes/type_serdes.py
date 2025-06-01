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


from dataclasses_json.core import Json

from gravitino.api.types.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.api.types.json_serdes.base import JsonSerializable
from gravitino.api.types.type import Type


class TypeSerdes(JsonSerializable[Type]):
    """Custom JSON serializer for Gravitino Type objects."""

    @classmethod
    def serialize(cls, data_type: Type) -> Json:
        """Serialize the given Gravitino Type.

        Args:
            data (Type): The Gravitino Type to be serialized.

        Returns:
            Json: The serialized data corresponding to the given Gravitino Type.
        """

        return SerdesUtils.write_data_type(data_type)

    @classmethod
    def deserialize(cls, data: Json) -> Type:
        """Deserialize the given data to a Gravitino Type.

        Args:
            data (Json): The data to be deserialized.

        Returns:
            Type: The deserialized Gravitino Type.
        """

        return SerdesUtils.read_data_type(data)
