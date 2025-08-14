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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplateRegisterRequest {

  @Test
  public void testJobTemplateRegisterRequestSerDe() throws JsonProcessingException {
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

    JobTemplateRegisterRequest request = new JobTemplateRegisterRequest(shellJobTemplateDTO);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    JobTemplateRegisterRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, JobTemplateRegisterRequest.class);
    Assertions.assertEquals(shellJobTemplateDTO, deserRequest.getJobTemplate());

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

    JobTemplateRegisterRequest sparkRequest = new JobTemplateRegisterRequest(sparkJobTemplateDTO);

    String sparkSerJson = JsonUtils.objectMapper().writeValueAsString(sparkRequest);
    JobTemplateRegisterRequest sparkDeserRequest =
        JsonUtils.objectMapper().readValue(sparkSerJson, JobTemplateRegisterRequest.class);

    Assertions.assertEquals(sparkJobTemplateDTO, sparkDeserRequest.getJobTemplate());
  }
}
