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
from enum import Enum


class JobHandle(ABC):
    class Status(Enum):
        QUEUED = "QUEUED"
        STARTED = "STARTED"
        FAILED = "FAILED"
        SUCCEEDED = "SUCCEEDED"
        CANCELLED = "CANCELLED"

    class Listener(ABC):
        @abstractmethod
        def on_job_queued(self):
            pass

        @abstractmethod
        def on_job_started(self):
            pass

        @abstractmethod
        def on_job_failed(self):
            pass

        @abstractmethod
        def on_job_succeeded(self):
            pass

    @abstractmethod
    def job_name(self) -> str:
        pass

    @abstractmethod
    def job_id(self) -> str:
        pass

    @abstractmethod
    def job_status(self) -> Status:
        pass

    @abstractmethod
    def add_listener(self, listener: Listener):
        pass
