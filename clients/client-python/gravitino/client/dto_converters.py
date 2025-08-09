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

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.api.job.job_template import JobTemplate, JobType
from gravitino.api.job.shell_job_template import ShellJobTemplate
from gravitino.api.job.spark_job_template import SparkJobTemplate
from gravitino.client.fileset_catalog import FilesetCatalog
from gravitino.client.generic_model_catalog import GenericModelCatalog
from gravitino.dto.catalog_dto import CatalogDTO
from gravitino.dto.job.job_template_dto import JobTemplateDTO
from gravitino.dto.job.shell_job_template_dto import ShellJobTemplateDTO
from gravitino.dto.job.spark_job_template_dto import SparkJobTemplateDTO
from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest
from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.api.metalake_change import MetalakeChange
from gravitino.utils import HTTPClient
from gravitino.namespace import Namespace


class DTOConverters:
    """Utility class for converting between DTOs and domain objects."""

    @staticmethod
    def to_metalake_update_request(change: MetalakeChange) -> object:
        # Assuming MetalakeUpdateRequest has similar nested class structure for requests
        if isinstance(change, MetalakeChange.RenameMetalake):
            return MetalakeUpdateRequest.RenameMetalakeRequest(change.new_name())
        if isinstance(change, MetalakeChange.UpdateMetalakeComment):
            return MetalakeUpdateRequest.UpdateMetalakeCommentRequest(
                change.new_comment()
            )
        if isinstance(change, MetalakeChange.SetProperty):
            return MetalakeUpdateRequest.SetMetalakePropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, MetalakeChange.RemoveProperty):
            return MetalakeUpdateRequest.RemoveMetalakePropertyRequest(
                change.property()
            )

        raise ValueError(f"Unknown change type: {type(change).__name__}")

    @staticmethod
    def to_catalog(metalake: str, catalog: CatalogDTO, client: HTTPClient):
        namespace = Namespace.of(metalake)
        if catalog.type() == Catalog.Type.FILESET:
            return FilesetCatalog(
                namespace=namespace,
                name=catalog.name(),
                catalog_type=catalog.type(),
                provider=catalog.provider(),
                comment=catalog.comment(),
                properties=catalog.properties(),
                audit=catalog.audit_info(),
                rest_client=client,
            )

        if catalog.type() == Catalog.Type.MODEL:
            return GenericModelCatalog(
                namespace=namespace,
                name=catalog.name(),
                catalog_type=catalog.type(),
                provider=catalog.provider(),
                comment=catalog.comment(),
                properties=catalog.properties(),
                audit=catalog.audit_info(),
                rest_client=client,
            )

        raise NotImplementedError("Unsupported catalog type: " + str(catalog.type()))

    @staticmethod
    def to_catalog_update_request(change: CatalogChange):
        if isinstance(change, CatalogChange.RenameCatalog):
            return CatalogUpdateRequest.RenameCatalogRequest(change.new_name())
        if isinstance(change, CatalogChange.UpdateCatalogComment):
            return CatalogUpdateRequest.UpdateCatalogCommentRequest(
                change.new_comment()
            )
        if isinstance(change, CatalogChange.SetProperty):
            # TODO
            # pylint: disable=too-many-function-args
            return CatalogUpdateRequest.SetCatalogPropertyRequest(
                change.property(), change.value()
            )
        if isinstance(change, CatalogChange.RemoveProperty):
            return CatalogUpdateRequest.RemoveCatalogPropertyRequest(
                change.get_property()
            )

        raise ValueError(f"Unknown change type: {type(change).__name__}")

    @staticmethod
    def _to_shell_job_template_dto(dto: JobTemplateDTO) -> ShellJobTemplateDTO:
        if isinstance(dto, ShellJobTemplateDTO):
            return dto

        raise TypeError(
            f"The provided JobTemplateDTO is not of type ShellJobTemplateDTO: {type(dto)}"
        )

    @staticmethod
    def _to_spark_job_template_dto(dto: JobTemplateDTO) -> SparkJobTemplateDTO:
        if isinstance(dto, SparkJobTemplateDTO):
            return dto

        raise TypeError(
            f"The provided JobTemplateDTO is not of type SparkJobTemplateDTO: {type(dto)}"
        )

    @staticmethod
    def _to_shell_job_template(template: JobTemplate) -> ShellJobTemplate:
        if isinstance(template, ShellJobTemplate):
            return template

        raise TypeError(
            f"The provided JobTemplate is not of type ShellJobTemplate: {type(template)}"
        )

    @staticmethod
    def _to_spark_job_template(template: JobTemplate) -> SparkJobTemplate:
        if isinstance(template, SparkJobTemplate):
            return template

        raise TypeError(
            f"The provided JobTemplate is not of type SparkJobTemplate: {type(template)}"
        )

    @staticmethod
    def from_job_template_dto(dto: JobTemplateDTO) -> JobTemplate:
        if dto.job_type() == JobType.SHELL:
            shell_dto = DTOConverters._to_shell_job_template_dto(dto)
            return (
                ShellJobTemplate.builder()
                .with_name(shell_dto.name())
                .with_comment(shell_dto.comment())
                .with_executable(shell_dto.executable())
                .with_arguments(shell_dto.arguments())
                .with_environments(shell_dto.environments())
                .with_custom_fields(shell_dto.custom_fields())
                .with_scripts(shell_dto.scripts())
                .build()
            )

        if dto.job_type() == JobType.SPARK:
            spark_dto = DTOConverters._to_spark_job_template_dto(dto)
            return (
                SparkJobTemplate.builder()
                .with_name(spark_dto.name())
                .with_comment(spark_dto.comment())
                .with_executable(spark_dto.executable())
                .with_arguments(spark_dto.arguments())
                .with_environments(spark_dto.environments())
                .with_custom_fields(spark_dto.custom_fields())
                .with_class_name(spark_dto.class_name())
                .with_jars(spark_dto.jars())
                .with_files(spark_dto.files())
                .with_archives(spark_dto.archives())
                .with_configs(spark_dto.configs())
                .build()
            )

        raise ValueError(f"Unsupported job type: {dto.job_type()}")

    @staticmethod
    def to_job_template_dto(template: JobTemplate) -> JobTemplateDTO:
        if isinstance(template, ShellJobTemplate):
            shell_template = DTOConverters._to_shell_job_template(template)
            return ShellJobTemplateDTO(
                _job_type=shell_template.job_type(),
                _name=shell_template.name,
                _comment=shell_template.comment,
                _executable=shell_template.executable,
                _arguments=shell_template.arguments,
                _environments=shell_template.environments,
                _custom_fields=shell_template.custom_fields,
                _scripts=shell_template.scripts,
            )

        if isinstance(template, SparkJobTemplate):
            spark_template = DTOConverters._to_spark_job_template(template)
            return SparkJobTemplateDTO(
                _job_type=spark_template.job_type(),
                _name=spark_template.name,
                _comment=spark_template.comment,
                _executable=spark_template.executable,
                _arguments=spark_template.arguments,
                _environments=spark_template.environments,
                _custom_fields=spark_template.custom_fields,
                _class_name=spark_template.class_name,
                _jars=spark_template.jars,
                _files=spark_template.files,
                _archives=spark_template.archives,
                _configs=spark_template.configs,
            )

        raise ValueError(f"Unsupported job type: {type(template)}")
