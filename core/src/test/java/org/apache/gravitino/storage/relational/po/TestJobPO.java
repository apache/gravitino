/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.po;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobPO {

  @Test
  public void testJobTemplatePO() {
    JobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName("shell-job-template")
            .withComment("This is a shell job template")
            .withExecutable("/bin/echo")
            .withArguments(Lists.newArrayList("Hello, World!"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withScripts(Lists.newArrayList("/path/to/script.sh"))
            .build();

    JobTemplateEntity shellTemplateEntity =
        JobTemplateEntity.builder()
            .withName(shellJobTemplate.name())
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(shellJobTemplate))
            .withComment(shellJobTemplate.comment())
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withId(1L)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobTemplatePO.JobTemplatePOBuilder builder = JobTemplatePO.builder().withMetalakeId(1L);

    JobTemplatePO jobTemplatePO =
        JobTemplatePO.initializeJobTemplatePO(shellTemplateEntity, builder);
    JobTemplateEntity resultEntity =
        JobTemplatePO.fromJobTemplatePO(jobTemplatePO, NamespaceUtil.ofJobTemplate("test"));

    Assertions.assertEquals(shellTemplateEntity.name(), resultEntity.name());
    Assertions.assertEquals(shellTemplateEntity.templateContent(), resultEntity.templateContent());
    Assertions.assertEquals(shellTemplateEntity.comment(), resultEntity.comment());
    Assertions.assertEquals(shellTemplateEntity.namespace(), resultEntity.namespace());
    Assertions.assertEquals(shellTemplateEntity.id(), resultEntity.id());
    Assertions.assertEquals(
        shellTemplateEntity.auditInfo().creator(), resultEntity.auditInfo().creator());

    JobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName("spark-job-template")
            .withComment("This is a spark job template")
            .withExecutable("/path/to/spark-demo.jar")
            .withClassName("org.example.SparkDemo")
            .withArguments(
                Lists.newArrayList("--input", "/path/to/input", "--output", "/path/to/output"))
            .withEnvironments(ImmutableMap.of("SPARK_ENV_VAR", "value"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withJars(Lists.newArrayList("/path/to/dependency.jar"))
            .withFiles(Lists.newArrayList("/path/to/config.yaml"))
            .withArchives(Lists.newArrayList("/path/to/archive.zip"))
            .build();

    JobTemplateEntity sparkTemplateEntity =
        JobTemplateEntity.builder()
            .withName(sparkJobTemplate.name())
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(sparkJobTemplate))
            .withComment(sparkJobTemplate.comment())
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withId(2L)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobTemplatePO.JobTemplatePOBuilder builder2 = JobTemplatePO.builder().withMetalakeId(1L);
    JobTemplatePO sparkJobTemplatePO =
        JobTemplatePO.initializeJobTemplatePO(sparkTemplateEntity, builder2);

    JobTemplateEntity resultSparkEntity =
        JobTemplatePO.fromJobTemplatePO(sparkJobTemplatePO, NamespaceUtil.ofJobTemplate("test"));

    Assertions.assertEquals(sparkTemplateEntity.name(), resultSparkEntity.name());
    Assertions.assertEquals(
        sparkTemplateEntity.templateContent(), resultSparkEntity.templateContent());
    Assertions.assertEquals(sparkTemplateEntity.comment(), resultSparkEntity.comment());
    Assertions.assertEquals(sparkTemplateEntity.namespace(), resultSparkEntity.namespace());
    Assertions.assertEquals(sparkTemplateEntity.id(), resultSparkEntity.id());
    Assertions.assertEquals(
        sparkTemplateEntity.auditInfo().creator(), resultSparkEntity.auditInfo().creator());
  }

  @Test
  public void testJobPO() {
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobPO.JobPOBuilder builder = JobPO.builder().withMetalakeId(1L);
    JobPO jobPO = JobPO.initializeJobPO(jobEntity, builder);
    JobEntity resultEntity = JobPO.fromJobPO(jobPO, NamespaceUtil.ofJob("test"));

    Assertions.assertEquals(jobEntity.id(), resultEntity.id());
    Assertions.assertEquals(jobEntity.jobExecutionId(), resultEntity.jobExecutionId());
    Assertions.assertEquals(jobEntity.jobTemplateName(), resultEntity.jobTemplateName());
    Assertions.assertEquals(jobEntity.status(), resultEntity.status());
    Assertions.assertEquals(jobEntity.namespace(), resultEntity.namespace());
    Assertions.assertEquals(jobEntity.auditInfo().creator(), resultEntity.auditInfo().creator());
  }
}
