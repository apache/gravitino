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

from abc import ABC, abstractmethod
from typing import List
from gravitino.api.credential.credential import Credential


class SupportsCredentials(ABC):
    """Represents interface to get credentials."""

    @abstractmethod
    def get_credentials(self) -> List[Credential]:
        """Retrieves an array of Credential objects.

        Returns:
        An array of Credential objects. In most cases the array only contains
        one credential. If the object like Fileset contains multiple locations
        for different storages like HDFS, S3, the array will contain multiple
        credentials. The array could be empty if you request a credential for
        a catalog but the credential provider couldn't generate the credential
        for the catalog, like S3 token credential provider only generate
        credential for the specific object like Fileset,Table. There will be at
         most one credential for one credential type.
        """
        pass
