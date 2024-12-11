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

from typing import List, ClassVar

from gravitino.api.metadata_object import MetadataObject


class MetadataObjectImpl(MetadataObject):

    _DOT: ClassVar[str] = "."

    def __init__(self, names: List[str], metadata_object_type: MetadataObject.Type):
        self._name = self._DOT.join(names)
        self._metadata_object_type = metadata_object_type

    def type(self) -> MetadataObject.Type:
        return self._metadata_object_type

    def name(self) -> str:
        return self._name
