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
package org.apache.gravitino.job.local;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobManager;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class TestLocalJobExecutor {

  private static JobExecutor jobExecutor;

  private static JobTemplateEntity jobTemplateEntity;

  private static File workingDir;

  @BeforeAll
  public static void setUpClass() throws IOException {
    jobExecutor = new LocalJobExecutor();
    jobExecutor.initialize(Collections.emptyMap());

    URL testJobScriptUrl = TestLocalJobExecutor.class.getResource("/test-job.sh");
    Assertions.assertNotNull(testJobScriptUrl);
    File testJobScriptFile = new File(testJobScriptUrl.getFile());

    URL commonScriptUrl = TestLocalJobExecutor.class.getResource("/common.sh");
    Assertions.assertNotNull(commonScriptUrl);
    File commonScriptFile = new File(commonScriptUrl.getFile());

    JobTemplateEntity.TemplateContent templateContent =
        JobTemplateEntity.TemplateContent.builder()
            .withExecutable(testJobScriptFile.getAbsolutePath())
            .withArguments(Lists.newArrayList("{{arg1}}", "{{arg2}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "{{var}}"))
            .withJobType(JobTemplate.JobType.SHELL)
            .withScripts(Lists.newArrayList(commonScriptFile.getAbsolutePath()))
            .withCustomFields(Collections.emptyMap())
            .build();
    jobTemplateEntity =
        JobTemplateEntity.builder()
            .withId(1L)
            .withName("test-job-template")
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withComment("test")
            .withTemplateContent(templateContent)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
  }

  @AfterAll
  public static void tearDownClass() throws IOException {
    if (jobExecutor != null) {
      jobExecutor.close();
      jobExecutor = null;
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    workingDir = Files.createTempDirectory("gravitino-test-local-job-executor").toFile();
  }

  @BeforeEach
  public void tearDown() throws IOException {
    if (workingDir != null && workingDir.exists()) {
      FileUtils.deleteDirectory(workingDir);
    }
  }

  @Test
  public void testSubmitJobSuccessfully() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Assertions.assertNotNull(jobId);

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    String output = FileUtils.readFileToString(new File(workingDir, "output.log"), "UTF-8");
    Assertions.assertTrue(output.contains("value1"));
    Assertions.assertTrue(output.contains("success"));
    Assertions.assertTrue(output.contains("value3"));
    Assertions.assertTrue(output.contains("in common script"));

    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, jobExecutor.getJobStatus(jobId));
  }

  @Test
  public void testSubmitJobFailure() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "fail",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Assertions.assertNotNull(jobId);

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.FAILED);

    String output = FileUtils.readFileToString(new File(workingDir, "output.log"), "UTF-8");
    Assertions.assertTrue(output.contains("value1"));
    Assertions.assertTrue(output.contains("fail"));
    Assertions.assertTrue(output.contains("value3"));
    Assertions.assertTrue(output.contains("in common script"));

    Assertions.assertEquals(JobHandle.Status.FAILED, jobExecutor.getJobStatus(jobId));
  }

  @Test
  public void testCancelJob() throws InterruptedException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Assertions.assertNotNull(jobId);
    // sleep a while to ensure the job is running.
    Thread.sleep(1000);

    jobExecutor.cancelJob(jobId);

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.CANCELLED);

    Assertions.assertEquals(JobHandle.Status.CANCELLED, jobExecutor.getJobStatus(jobId));

    // Cancelling a job that is already cancelled.
    Assertions.assertDoesNotThrow(() -> jobExecutor.cancelJob(jobId));
    Assertions.assertEquals(JobHandle.Status.CANCELLED, jobExecutor.getJobStatus(jobId));
  }

  @Test
  public void testCancelSucceededJob() {
    // Cancelling a job that is already succeeded.
    Map<String, String> successJobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate successTemplate =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, successJobConf, workingDir);
    String successJobId = jobExecutor.submitJob(successTemplate);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(successJobId) == JobHandle.Status.SUCCEEDED);

    Assertions.assertDoesNotThrow(() -> jobExecutor.cancelJob(successJobId));
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, jobExecutor.getJobStatus(successJobId));
  }

  @Test
  public void testCancelFailedJob() {
    // Cancelling a job that is already failed.
    Map<String, String> failJobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "fail",
            "var", "value3");

    JobTemplate failTemplate =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, failJobConf, workingDir);
    String failJobId = jobExecutor.submitJob(failTemplate);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(failJobId) == JobHandle.Status.FAILED);

    Assertions.assertDoesNotThrow(() -> jobExecutor.cancelJob(failJobId));
    Assertions.assertEquals(JobHandle.Status.FAILED, jobExecutor.getJobStatus(failJobId));
  }
}
