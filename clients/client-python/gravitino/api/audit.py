"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC, abstractmethod
from datetime import datetime


class Audit(ABC):
    """Represents the audit information of an entity."""

    @abstractmethod
    def creator(self) -> str:
        """The creator of the entity.

        Returns:
             the creator of the entity.
        """
        pass

    @abstractmethod
    def create_time(self) -> datetime:
        """The creation time of the entity.

        Returns:
             The creation time of the entity.
        """
        pass

    @abstractmethod
    def last_modifier(self) -> str:
        """
        Returns:
             The last modifier of the entity.
        """
        pass

    @abstractmethod
    def last_modified_time(self) -> datetime:
        """
        Returns:
             The last modified time of the entity.
        """
        pass
