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


from abc import abstractmethod
from typing import Optional, Dict, List
from gravitino.api.auditable import Auditable


class ModelVersion(Auditable):
    """
    An interface representing a single model checkpoint under a model `Model`. A model version
    is a snapshot at a point of time of a model artifact in ML. Users can link a model version to a
    registered model.
    """

    @abstractmethod
    def version(self) -> int:
        """
        The version of this model object. The version number is an integer number starts from 0. Each
        time the model checkpoint / snapshot is linked to the registered, the version number will be
        increased by 1.

        Returns:
            The version of the model object.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """
        The comment of this model version. This comment can be different from the comment of the model
        to provide more detailed information about this version.

        Returns:
            The comment of the model version. None is returned if no comment is set.
        """
        pass

    @abstractmethod
    def aliases(self) -> List[str]:
        """
        The aliases of this model version. The aliases are the alternative names of the model version.
        The aliases are optional. The aliases are unique for a model version. If the alias is already
        set to one model version, it cannot be set to another model version.

        Returns:
            The aliases of the model version.
        """
        pass

    @abstractmethod
    def uri(self) -> str:
        """
        The URI of the model artifact. The URI is the location of the model artifact. The URI can be a
        file path or a remote URI.

        Returns:
            The URI of the model artifact.
        """
        pass

    def properties(self) -> Dict[str, str]:
        """
        The properties of the model version. The properties are key-value pairs that can be used to
        store additional information of the model version. The properties are optional.

        Returns:
            The properties of the model version. An empty dictionary is returned if no properties are set.
        """
        pass
