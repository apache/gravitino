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

from typing import Dict, Optional
from abc import abstractmethod

from gravitino.api.auditable import Auditable


class Model(Auditable):
    """An interface representing an ML model under a schema `Namespace`. A model is a metadata
    object that represents the model artifact in ML. Users can register a model object in Gravitino
    to manage the model metadata. The typical use case is to manage the model in ML lifecycle with a
    unified way in Gravitino, and access the model artifact with a unified identifier. Also, with
    the model registered in Gravitino, users can govern the model with Gravitino's unified audit,
    tag, and role management.

    The difference of Model and tabular data is that the model is schema-free, and the main
    property of the model is the model artifact URL. The difference compared to the fileset is that
    the model is versioned, and the model object contains the version information.
    """

    @abstractmethod
    def name(self) -> str:
        """
        Returns:
            Name of the model object.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """The comment of the model object. This is the general description of the model object.
        User can still add more detailed information in the model version.

        Returns:
            The comment of the model object. None is returned if no comment is set.
        """
        pass

    def properties(self) -> Dict[str, str]:
        """The properties of the model object. The properties are key-value pairs that can be used
        to store additional information of the model object. The properties are optional.

        Users can still specify the properties in the model version for different information.

        Returns:
            The properties of the model object. An empty dictionary is returned if no properties are set.
        """
        pass

    @abstractmethod
    def latest_version(self) -> int:
        """The latest version of the model object. The latest version is the version number of the
        latest model checkpoint / snapshot that is linked to the registered model.

        Returns:
            The latest version of the model object.
        """
        pass
