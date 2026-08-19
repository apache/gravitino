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
from datetime import datetime
from enum import Enum
from typing import Optional


class JobHandle(ABC):
    class Status(Enum):
        QUEUED = "queued"
        STARTED = "started"
        FAILED = "failed"
        SUCCEEDED = "succeeded"
        CANCELLING = "cancelling"
        CANCELLED = "cancelled"

        @classmethod
        def job_status_serialize(cls, status: "JobHandle.Status") -> str:
            """Serializes the job status to a string."""
            return status.value.lower()

        @classmethod
        def job_status_deserialize(cls, status: str) -> "JobHandle.Status":
            """Deserializes a string to a job status."""
            for m in cls:
                if m.value.lower() == status.lower():
                    return m
            raise ValueError(f"Unknown job status: {status}")

    @abstractmethod
    def job_template_name(self) -> str:
        pass

    @abstractmethod
    def job_id(self) -> str:
        pass

    @abstractmethod
    def job_status(self) -> Status:
        pass

    def finished_at(self) -> Optional[datetime]:
        """Returns the time the job finished execution, or ``None`` if the job has not finished
        execution yet.
        """
        raise NotImplementedError("finished_at is not implemented")
