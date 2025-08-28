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

from fastmcp import Context, FastMCP


def load_job_tool(mcp: FastMCP):
    @mcp.tool(tags={"job"})
    async def list_of_jobs(
        ctx: Context,
        job_template_name: str = None,
    ) -> str:
        """
        Get a list of jobs in the metalake. If `job_template_name` is provided,
        the method will only return jobs that created from that specific job template.
        If no job template name is provided, all jobs will be returned.

        Parameters:
            ctx (Context): The context object containing Gravitino context.
            job_template_name (str): The name of the job template to filter jobs by.
                                     If empty or not provided, all jobs are returned.
        returns:
            str: A JSON string representing an array of job objects with the following structure:
                [
                  {
                    "jobId": "job-3210182983329751377",
                    "jobTemplateName": "shell_test",
                    "status": "succeeded",
                    "audit": {
                      "creator": "anonymous",
                      "createTime": "2025-08-13T11:50:27.019723Z",
                      "lastModifier": "anonymous",
                      "lastModifiedTime": "2025-08-13T11:51:43.038661Z"
                    }
                  }
                ]

                jobId: The unique identifier of the job.
                jobTemplateName: The name of the job template used to create the job.
                status: The current status of the job (e.g., "succeeded", "failed").
                audit: An object containing audit information, including creator and creation time.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_job_operation().list_of_jobs(job_template_name)

    @mcp.tool(tags={"job"})
    async def get_job_by_id(
        ctx: Context,
        job_id: str,
    ) -> str:
        """
        Get a job by its ID.
        Parameters:
            ctx (Context): The context object containing Gravitino context.
            job_id (str): The ID of the job to retrieve.

        Returns:
            str: A JSON string representing the job object with the following structure:
                {
                  "jobId": "job-3210182983329751377",
                  "jobTemplateName": "shell_test",
                  "status": "succeeded",
                  "audit": {
                    "creator": "anonymous",
                    "createTime": "2025-08-13T11:50:27.019723Z",
                    "lastModifier": "anonymous",
                    "lastModifiedTime": "2025-08-13T11:51:43.038661Z"
                  }
                }

                jobId: The unique identifier of the job.
                jobTemplateName: The name of the job template used to create the job.
                status: The current status of the job (e.g., "succeeded", "failed").
                audit: An object containing audit information, including creator and creation time.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_job_operation().get_job_by_id(job_id)

    @mcp.tool(tags={"job"})
    async def list_of_job_templates(
        ctx: Context,
    ) -> str:
        """
        Get a list of job templates in the metalake.

        Parameters:
            ctx (Context): The context object containing Gravitino context.

        Returns:
            str: A JSON string representing an array of job template objects with the following structure:
                [
                  {
                    "jobType": "shell",
                    "name": "shell_test",
                    "comment": "this is a shell test",
                    "executable": "/tmp/test/test.sh",
                    "arguments": [],
                    "environments": {},
                    "customFields": {},
                    "audit": {
                      "creator": "anonymous",
                      "createTime": "2025-08-13T11:45:50.124843Z"
                    },
                    "scripts": []
                  }
                ]

                name: The name of the job template to get.
                jobType: The type of the job (e.g., "shell").
                comment: A comment describing the job template.
                executable: The path to the executable script for the job.
                arguments: A list of arguments for the job.
                environments: A dictionary of environment variables for the job.
                customFields: A dictionary of custom fields for the job.
                audit: An object containing audit information, including creator and creation time.
                scripts: A list of scripts associated with the job template and can be called by the executable.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_job_operation().list_of_job_templates()

    @mcp.tool(tags={"job"})
    async def get_job_template_by_name(
        ctx: Context,
        name: str,
    ) -> str:
        """
        Get a job template by its name.

        Parameters:
            ctx (Context): The context object containing Gravitino context.
            name (str): The name of the job template to retrieve.

        Returns:
            str: A JSON string representing the job template object with the following structure:
                {
                  "jobType": "shell",
                  "name": "shell_test",
                  "comment": "this is a shell test",
                  "executable": "/tmp/test/test.sh",
                  "arguments": [],
                  "environments": {},
                  "customFields": {},
                  "audit": {
                    "creator": "anonymous",
                    "createTime": "2025-08-13T11:45:50.124843Z"
                  },
                  "scripts": []
                }

                name: The name of the job template to get.
                jobType: The type of the job (e.g., "shell").
                comment: A comment describing the job template.
                executable: The path to the executable script for the job.
                arguments: A list of arguments for the job.
                environments: A dictionary of environment variables for the job.
                customFields: A dictionary of custom fields for the job.
                audit: An object containing audit information, including creator and creation time.
                scripts: A list of scripts associated with the job template and can be called by the executable.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_job_operation().get_job_template_by_name(name)

    @mcp.tool(tags={"job"})
    async def run_job(
        ctx: Context,
        job_template_name: str,
        job_config: dict,
    ) -> str:
        """
        Run a job using a specified job template and configuration.

        Parameters:
            ctx (Context): The context object containing Gravitino context.
            job_template_name (str): The name of the job template to use for running the job.
            job_config (dict): A dictionary containing the configuration for the job.

        Returns:
            str: A JSON string representing the created job object.

        Example Return Value:
            {
              "jobId": "job-3650896936678278254",
              "jobTemplateName": "shell_test",
              "status": "queued",
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-14T08:50:33.098029Z"
              }
            }
            jobId: The unique identifier of the job.
            jobTemplateName: The name of the job template used to create the job.
            status: The current status of the job (e.g., "queued", "running").
            audit: An object containing audit information, including creator and creation time.
        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_job_operation().run_job(
            job_template_name, job_config
        )

    @mcp.tool(tags={"job"})
    async def cancel_job(
        ctx: Context,
        job_id: str,
    ) -> str:
        """
        Cancel a job by its ID. The ID should be the one returned by the `run_job` method.

        Parameters:
            ctx (Context): The context object containing Gravitino context.
            job_id (str): The ID of the job to cancel.

        Returns:
            str: A JSON string representing the current status of the job to be cancelled.

        Example Return Value:
            {
              "jobId": "job-3650896936678278254",
              "jobTemplateName": "shell_test",
              "status": "succeeded",
              "audit": {
                "creator": "anonymous",
                "createTime": "2025-08-14T08:50:33.098029Z",
                "lastModifier": "anonymous",
                "lastModifiedTime": "2025-08-14T08:52:10.965113Z"
              }
            }
            jobId: The unique identifier of the job.
            jobTemplateName: The name of the job template used to create the job.
            status: The current status of the job (e.g., "succeeded", "failed", "cancelled").
                    If the job was successfully cancelled, the status will be "cancelled". If the
                    job was already completed, the status will reflect that.
            audit: An object containing audit information, including creator, creation time,
                    last modifier, and last modified time.

        """
        client = ctx.request_context.lifespan_context.rest_client()
        return await client.as_job_operation().cancel_job(job_id)
