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


class JobOperation(ABC):
    """
    Abstract base class for Gravitino job operations.
    """

    @abstractmethod
    async def get_job_by_id(self, job_id: str) -> str:
        """
        Load a job by its ID.

        Args:
            job_id: ID of the job to get

        Returns:
            str: JSON-formatted string containing the job information
        """
        pass

    @abstractmethod
    async def get_list_of_jobs(self, job_template_name: str) -> str:
        """
        Retrieve the list of jobs within the metalake

        Args:
            job_template_name: Name of the job template to filter jobs if needed.
            The parameter can be an empty string/None to retrieve all jobs.

        Returns:
            str: JSON-formatted string containing job list information
        """
        pass

    @abstractmethod
    async def get_list_of_job_templates(self) -> str:
        """
        Retrieve the list of job templates within the metalake

        Returns:
            str: JSON-formatted string containing job template list information
        """
        pass

    @abstractmethod
    async def get_job_template_by_name(self, name: str) -> str:
        """
        Load a job template by its name.

        Args:
            name: Name of the job template to get

        Returns:
            str: JSON-formatted string containing the job template information
        """
        pass
