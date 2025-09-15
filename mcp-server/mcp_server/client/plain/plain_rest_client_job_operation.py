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

from httpx import AsyncClient

from mcp_server.client.job_operation import JobOperation
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.utils import extract_content_from_response


class PlainRESTClientJobOperation(JobOperation):
    def __init__(self, metalake_name: str, rest_client: AsyncClient):
        self.metalake_name = metalake_name
        self.rest_client = rest_client

    async def get_job_by_id(self, job_id: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/jobs/runs/{job_id}"
        )
        return extract_content_from_response(response, "job", {})

    async def list_of_jobs(self, job_template_name: str) -> str:
        url = f"/api/metalakes/{self.metalake_name}/jobs/runs"
        if job_template_name:
            url += f"?jobTemplateName={job_template_name}"

        response = await self.rest_client.get(url)
        return extract_content_from_response(response, "jobs", [])

    async def get_job_template_by_name(self, name: str) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/jobs/templates/{name}"
        )
        return extract_content_from_response(response, "jobTemplate", {})

    async def list_of_job_templates(self) -> str:
        response = await self.rest_client.get(
            f"/api/metalakes/{self.metalake_name}/jobs/templates?details=true"
        )
        return extract_content_from_response(response, "jobTemplates", [])

    async def run_job(self, job_template_name: str, job_config: dict) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{self.metalake_name}/jobs/runs",
            json={
                "jobTemplateName": job_template_name,
                "jobConf": job_config,
            },
        )
        return extract_content_from_response(response, "job", {})

    async def cancel_job(self, job_id: str) -> str:
        response = await self.rest_client.post(
            f"/api/metalakes/{self.metalake_name}/jobs/runs/{job_id}"
        )
        if response.status_code != 200:
            raise GravitinoException(
                f"Failed to cancel job {job_id}: {response.text}"
            )

        return extract_content_from_response(response, "job", {})
