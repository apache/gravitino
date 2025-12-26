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
package org.apache.gravitino.dto.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplateDTO {

  @Test
  public void testShellJobTemplateDTO() throws JsonProcessingException {
    JobTemplateDTO shellJobTemplateDTO =
        ShellJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withName("testShellJob")
            .withComment("This is a test shell job template")
            .withExecutable("/path/to/shell")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withScripts(Lists.newArrayList("/path/to/script1.sh", "/path/to/script2.sh"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(shellJobTemplateDTO::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(shellJobTemplateDTO);
    JobTemplateDTO deserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(serJson, JobTemplateDTO.class);
    Assertions.assertEquals(shellJobTemplateDTO, deserJobTemplateDTO);

    // Test comment is null
    ShellJobTemplateDTO nullCommentJobTemplateDTO =
        ShellJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withName("testShellJobNullComment")
            .withExecutable("/path/to/shell")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withScripts(Lists.newArrayList("/path/to/script1.sh", "/path/to/script2.sh"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullCommentJobTemplateDTO::validate);
    String nullCommentSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullCommentJobTemplateDTO);
    JobTemplateDTO nullCommentDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullCommentSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullCommentJobTemplateDTO, nullCommentDeserJobTemplateDTO);

    // Test arguments are null
    ShellJobTemplateDTO nullArgumentsJobTemplateDTO =
        ShellJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withName("testShellJobNullArguments")
            .withExecutable("/path/to/shell")
            .withEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withScripts(Lists.newArrayList("/path/to/script1.sh", "/path/to/script2.sh"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullArgumentsJobTemplateDTO::validate);
    String nullArgumentsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullArgumentsJobTemplateDTO);
    JobTemplateDTO nullArgumentsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullArgumentsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullArgumentsJobTemplateDTO, nullArgumentsDeserJobTemplateDTO);

    // Test environments are null
    ShellJobTemplateDTO nullEnvironmentsJobTemplateDTO =
        ShellJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withName("testShellJobNullEnvironments")
            .withExecutable("/path/to/shell")
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withScripts(Lists.newArrayList("/path/to/script1.sh", "/path/to/script2.sh"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullEnvironmentsJobTemplateDTO::validate);
    String nullEnvironmentsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullEnvironmentsJobTemplateDTO);
    JobTemplateDTO nullEnvironmentsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullEnvironmentsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullEnvironmentsJobTemplateDTO, nullEnvironmentsDeserJobTemplateDTO);

    // Test custom fields are null
    ShellJobTemplateDTO nullCustomFieldsJobTemplateDTO =
        ShellJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withName("testShellJobNullCustomFields")
            .withExecutable("/path/to/shell")
            .withScripts(Lists.newArrayList("/path/to/script1.sh", "/path/to/script2.sh"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullCustomFieldsJobTemplateDTO::validate);
    String nullCustomFieldsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullCustomFieldsJobTemplateDTO);
    JobTemplateDTO nullCustomFieldsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullCustomFieldsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullCustomFieldsJobTemplateDTO, nullCustomFieldsDeserJobTemplateDTO);

    // Test scripts are null
    ShellJobTemplateDTO nullScriptsJobTemplateDTO =
        ShellJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SHELL)
            .withName("testShellJobNullScripts")
            .withExecutable("/path/to/shell")
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullScriptsJobTemplateDTO::validate);
    String nullScriptsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullScriptsJobTemplateDTO);
    JobTemplateDTO nullScriptsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullScriptsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullScriptsJobTemplateDTO, nullScriptsDeserJobTemplateDTO);

    // Test name is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ShellJobTemplateDTO template =
              ShellJobTemplateDTO.builder()
                  .withJobType(JobTemplate.JobType.SHELL)
                  .withExecutable("/path/to/shell")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"name\" is required and cannot be empty");

    // Test executable is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ShellJobTemplateDTO template =
              ShellJobTemplateDTO.builder()
                  .withJobType(JobTemplate.JobType.SHELL)
                  .withName("testShellJobNullExecutable")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"executable\" is required and cannot be empty");

    // Test jobType is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ShellJobTemplateDTO template =
              ShellJobTemplateDTO.builder()
                  .withName("testShellJobNullJobType")
                  .withExecutable("/path/to/shell")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"jobType\" is required and cannot be null");
  }

  @Test
  public void testSparkJobTemplateDTO() throws JsonProcessingException {
    JobTemplateDTO sparkJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJob")
            .withComment("This is a test spark job template")
            .withExecutable("/path/to/spark-submit")
            .withArguments(Lists.newArrayList("--class", "com.example.Main"))
            .withEnvironments(ImmutableMap.of("SPARK_ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withClassName("com.example.Main")
            .withJars(Lists.newArrayList("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(sparkJobTemplateDTO::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(sparkJobTemplateDTO);
    JobTemplateDTO deserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(serJson, JobTemplateDTO.class);
    Assertions.assertEquals(sparkJobTemplateDTO, deserJobTemplateDTO);

    // Test comment is null
    SparkJobTemplateDTO nullCommentJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullComment")
            .withExecutable("/path/to/spark-submit")
            .withArguments(Lists.newArrayList("--class", "com.example.Main"))
            .withEnvironments(ImmutableMap.of("SPARK_ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withClassName("com.example.Main")
            .withJars(Lists.newArrayList("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullCommentJobTemplateDTO::validate);
    String nullCommentSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullCommentJobTemplateDTO);
    JobTemplateDTO nullCommentDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullCommentSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullCommentJobTemplateDTO, nullCommentDeserJobTemplateDTO);

    // Test arguments are null
    SparkJobTemplateDTO nullArgumentsJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullArguments")
            .withExecutable("/path/to/spark-submit")
            .withEnvironments(ImmutableMap.of("SPARK_ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withClassName("com.example.Main")
            .withJars(Lists.newArrayList("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullArgumentsJobTemplateDTO::validate);
    String nullArgumentsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullArgumentsJobTemplateDTO);
    JobTemplateDTO nullArgumentsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullArgumentsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullArgumentsJobTemplateDTO, nullArgumentsDeserJobTemplateDTO);

    // Test environments are null
    SparkJobTemplateDTO nullEnvironmentsJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullEnvironments")
            .withExecutable("/path/to/spark-submit")
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withClassName("com.example.Main")
            .withJars(Lists.newArrayList("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullEnvironmentsJobTemplateDTO::validate);
    String nullEnvironmentsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullEnvironmentsJobTemplateDTO);
    JobTemplateDTO nullEnvironmentsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullEnvironmentsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullEnvironmentsJobTemplateDTO, nullEnvironmentsDeserJobTemplateDTO);

    // Test custom fields are null
    SparkJobTemplateDTO nullCustomFieldsJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullCustomFields")
            .withExecutable("/path/to/spark-submit")
            .withClassName("com.example.Main")
            .withJars(Lists.newArrayList("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullCustomFieldsJobTemplateDTO::validate);
    String nullCustomFieldsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullCustomFieldsJobTemplateDTO);
    JobTemplateDTO nullCustomFieldsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullCustomFieldsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullCustomFieldsJobTemplateDTO, nullCustomFieldsDeserJobTemplateDTO);

    // Test Jars are null
    SparkJobTemplateDTO nullJarsJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullJars")
            .withExecutable("/path/to/spark-submit")
            .withClassName("com.example.Main")
            .withFiles(Lists.newArrayList("/path/to/file1.txt", "/path/to/file2.txt"))
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullJarsJobTemplateDTO::validate);
    String nullJarsSerJson = JsonUtils.objectMapper().writeValueAsString(nullJarsJobTemplateDTO);
    JobTemplateDTO nullJarsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullJarsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullJarsJobTemplateDTO, nullJarsDeserJobTemplateDTO);

    // Test files are null
    SparkJobTemplateDTO nullFilesJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullFiles")
            .withExecutable("/path/to/spark-submit")
            .withClassName("com.example.Main")
            .withArchives(Lists.newArrayList("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullFilesJobTemplateDTO::validate);
    String nullFilesSerJson = JsonUtils.objectMapper().writeValueAsString(nullFilesJobTemplateDTO);
    JobTemplateDTO nullFilesDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullFilesSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullFilesJobTemplateDTO, nullFilesDeserJobTemplateDTO);

    // Test archives are null
    SparkJobTemplateDTO nullArchivesJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullArchives")
            .withExecutable("/path/to/spark-submit")
            .withClassName("com.example.Main")
            .withConfigs(ImmutableMap.of("spark.executor.memory", "2g"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullArchivesJobTemplateDTO::validate);
    String nullArchivesSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullArchivesJobTemplateDTO);
    JobTemplateDTO nullArchivesDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullArchivesSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullArchivesJobTemplateDTO, nullArchivesDeserJobTemplateDTO);

    // Test configs are null
    SparkJobTemplateDTO nullConfigsJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullConfigs")
            .withExecutable("/path/to/spark-submit")
            .withClassName("com.example.Main")
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullConfigsJobTemplateDTO::validate);
    String nullConfigsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullConfigsJobTemplateDTO);
    JobTemplateDTO nullConfigsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullConfigsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullConfigsJobTemplateDTO, nullConfigsDeserJobTemplateDTO);

    // Test className is null
    SparkJobTemplateDTO nullClassNameJobTemplateDTO =
        SparkJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.SPARK)
            .withName("testSparkJobNullClassName")
            .withExecutable("/path/to/spark-submit")
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullClassNameJobTemplateDTO::validate);
    String nullClassNameSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullClassNameJobTemplateDTO);
    JobTemplateDTO nullClassNameDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullClassNameSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullClassNameJobTemplateDTO, nullClassNameDeserJobTemplateDTO);

    // Test name is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          SparkJobTemplateDTO template =
              SparkJobTemplateDTO.builder()
                  .withJobType(JobTemplate.JobType.SPARK)
                  .withExecutable("/path/to/spark-submit")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"name\" is required and cannot be empty");

    // Test executable is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          SparkJobTemplateDTO template =
              SparkJobTemplateDTO.builder()
                  .withJobType(JobTemplate.JobType.SPARK)
                  .withName("testSparkJobNullExecutable")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"executable\" is required and cannot be empty");

    // Test jobType is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          SparkJobTemplateDTO template =
              SparkJobTemplateDTO.builder()
                  .withName("testSparkJobNullJobType")
                  .withExecutable("/path/to/spark-submit")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"jobType\" is required and cannot be null");
  }

  @Test
  public void testHttpJobTemplateDTO() throws JsonProcessingException {
    JobTemplateDTO httpJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJob")
            .withComment("This is a test HTTP job template")
            .withExecutable("GET")
            .withArguments(Lists.newArrayList("arg1", "arg2"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(httpJobTemplateDTO::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(httpJobTemplateDTO);
    JobTemplateDTO deserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(serJson, JobTemplateDTO.class);
    Assertions.assertEquals(httpJobTemplateDTO, deserJobTemplateDTO);

    // Test comment is null
    HttpJobTemplateDTO nullCommentJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullComment")
            .withExecutable("POST")
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullCommentJobTemplateDTO::validate);
    String nullCommentSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullCommentJobTemplateDTO);
    JobTemplateDTO nullCommentDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullCommentSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullCommentJobTemplateDTO, nullCommentDeserJobTemplateDTO);

    // Test arguments are null
    HttpJobTemplateDTO nullArgumentsJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullArguments")
            .withExecutable("PUT")
            .withEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullArgumentsJobTemplateDTO::validate);
    String nullArgumentsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullArgumentsJobTemplateDTO);
    JobTemplateDTO nullArgumentsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullArgumentsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullArgumentsJobTemplateDTO, nullArgumentsDeserJobTemplateDTO);

    // Test environments are null
    HttpJobTemplateDTO nullEnvironmentsJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullEnvironments")
            .withExecutable("DELETE")
            .withCustomFields(ImmutableMap.of("customField1", "value1"))
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullEnvironmentsJobTemplateDTO::validate);
    String nullEnvironmentsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullEnvironmentsJobTemplateDTO);
    JobTemplateDTO nullEnvironmentsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullEnvironmentsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullEnvironmentsJobTemplateDTO, nullEnvironmentsDeserJobTemplateDTO);

    // Test custom fields are null
    HttpJobTemplateDTO nullCustomFieldsJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullCustomFields")
            .withExecutable("PATCH")
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullCustomFieldsJobTemplateDTO::validate);
    String nullCustomFieldsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullCustomFieldsJobTemplateDTO);
    JobTemplateDTO nullCustomFieldsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullCustomFieldsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullCustomFieldsJobTemplateDTO, nullCustomFieldsDeserJobTemplateDTO);

    // Test headers are null
    HttpJobTemplateDTO nullHeadersJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullHeaders")
            .withExecutable("HEAD")
            .withUrl("http://example.com/api")
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullHeadersJobTemplateDTO::validate);
    String nullHeadersSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullHeadersJobTemplateDTO);
    JobTemplateDTO nullHeadersDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullHeadersSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullHeadersJobTemplateDTO, nullHeadersDeserJobTemplateDTO);

    // Test body is null
    HttpJobTemplateDTO nullBodyJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullBody")
            .withExecutable("OPTIONS")
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullBodyJobTemplateDTO::validate);
    String nullBodySerJson = JsonUtils.objectMapper().writeValueAsString(nullBodyJobTemplateDTO);
    JobTemplateDTO nullBodyDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullBodySerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullBodyJobTemplateDTO, nullBodyDeserJobTemplateDTO);

    // Test query params are null
    HttpJobTemplateDTO nullQueryParamsJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullQueryParams")
            .withExecutable("TRACE")
            .withUrl("http://example.com/api")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullQueryParamsJobTemplateDTO::validate);
    String nullQueryParamsSerJson =
        JsonUtils.objectMapper().writeValueAsString(nullQueryParamsJobTemplateDTO);
    JobTemplateDTO nullQueryParamsDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullQueryParamsSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullQueryParamsJobTemplateDTO, nullQueryParamsDeserJobTemplateDTO);

    // Test url is null
    HttpJobTemplateDTO nullUrlJobTemplateDTO =
        HttpJobTemplateDTO.builder()
            .withJobType(JobTemplate.JobType.HTTP)
            .withName("testHttpJobNullUrl")
            .withExecutable("GET")
            .withHeaders(ImmutableMap.of("Content-Type", "application/json"))
            .withBody("{\"key\": \"value\"}")
            .withQueryParams(Lists.newArrayList("param1=value1", "param2=value2"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertDoesNotThrow(nullUrlJobTemplateDTO::validate);
    String nullUrlSerJson = JsonUtils.objectMapper().writeValueAsString(nullUrlJobTemplateDTO);
    JobTemplateDTO nullUrlDeserJobTemplateDTO =
        JsonUtils.objectMapper().readValue(nullUrlSerJson, JobTemplateDTO.class);
    Assertions.assertEquals(nullUrlJobTemplateDTO, nullUrlDeserJobTemplateDTO);

    // Test name is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          HttpJobTemplateDTO template =
              HttpJobTemplateDTO.builder()
                  .withJobType(JobTemplate.JobType.HTTP)
                  .withExecutable("GET")
                  .withUrl("http://example.com/api")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"name\" is required and cannot be empty");

    // Test executable is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          HttpJobTemplateDTO template =
              HttpJobTemplateDTO.builder()
                  .withJobType(JobTemplate.JobType.HTTP)
                  .withName("testHttpJobNullExecutable")
                  .withUrl("http://example.com/api")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"executable\" is required and cannot be empty");

    // Test jobType is null
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          HttpJobTemplateDTO template =
              HttpJobTemplateDTO.builder()
                  .withName("testHttpJobNullJobType")
                  .withExecutable("GET")
                  .withUrl("http://example.com/api")
                  .withAudit(
                      AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
                  .build();
          template.validate();
        },
        "\"jobType\" is required and cannot be null");
  }
}
