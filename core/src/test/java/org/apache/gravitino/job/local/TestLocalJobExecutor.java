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
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobManager;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @AfterEach
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
  public void testJobOwnership() throws IOException {
    LocalJobExecutor executor = (LocalJobExecutor) jobExecutor;
    Assertions.assertTrue(executor.isJobStateNodeLocal());
    Assertions.assertTrue(executor.executorId().matches("[0-9a-f]{8}"));

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(
            jobTemplateEntity,
            ImmutableMap.of("arg1", "value1", "arg2", "success", "var", "value3"),
            workingDir);
    String jobId = executor.submitJob(template);
    Assertions.assertTrue(
        jobId.matches("local-job-" + executor.executorId() + "-[0-9a-f-]{36}"), jobId);
    Assertions.assertTrue(executor.ownsJob(jobId));

    // Jobs submitted before the executor id was introduced aren't owned by any executor.
    Assertions.assertFalse(executor.ownsJob("local-job-" + UUID.randomUUID()));
    Assertions.assertFalse(executor.ownsJob(null));

    LocalJobExecutor anotherExecutor = new LocalJobExecutor();
    try {
      anotherExecutor.initialize(Collections.emptyMap());
      Assertions.assertNotEquals(executor.executorId(), anotherExecutor.executorId());
      Assertions.assertFalse(anotherExecutor.ownsJob(jobId));
      Assertions.assertThrows(NoSuchJobException.class, () -> anotherExecutor.getJobStatus(jobId));
    } finally {
      anotherExecutor.close();
    }

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> executor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);
  }

  @Test
  public void testRunJobsConcurrentlyUpToMaxRunningJobs() throws IOException {
    LocalJobExecutor executor = new LocalJobExecutor();
    try {
      executor.initialize(ImmutableMap.of(LocalJobExecutorConfigs.MAX_RUNNING_JOBS, "2"));
      String jobId1 = executor.submitJob(newSleepJobTemplate("sleep-1"));
      String jobId2 = executor.submitJob(newSleepJobTemplate("sleep-2"));
      String jobId3 = executor.submitJob(newSleepJobTemplate("sleep-3"));

      // Up to maxRunningJobs jobs run at the same time, the others wait in the queue.
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .until(
              () ->
                  executor.getJobStatus(jobId1) == JobHandle.Status.STARTED
                      && executor.getJobStatus(jobId2) == JobHandle.Status.STARTED);
      Awaitility.await()
          .during(1, TimeUnit.SECONDS)
          .atMost(2, TimeUnit.SECONDS)
          .until(() -> executor.getJobStatus(jobId3) == JobHandle.Status.QUEUED);
    } finally {
      // Closing the executor kills the running jobs.
      executor.close();
    }
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
  public void testSubmitSparkJobRejectedWhenSparkSubmitIsNotAvailable() throws IOException {
    File sparkHome = new File(workingDir, "spark");
    LocalJobExecutor exec = new LocalJobExecutor();
    exec.initialize(
        ImmutableMap.of(LocalJobExecutorConfigs.SPARK_HOME, sparkHome.getAbsolutePath()));

    try {
      SparkJobTemplate template =
          SparkJobTemplate.builder()
              .withName("spark-job")
              .withExecutable(new File(workingDir, "spark-demo.jar").getAbsolutePath())
              .withClassName("com.example.MainClass")
              .build();

      // spark-submit does not exist, the job is rejected at submission instead of being queued.
      IllegalArgumentException e =
          Assertions.assertThrows(IllegalArgumentException.class, () -> exec.submitJob(template));
      Assertions.assertTrue(e.getMessage().contains("spark-submit is not found or not executable"));

      // Once spark-submit is available, the same job is accepted.
      File sparkSubmit = new File(sparkHome, "bin/spark-submit");
      FileUtils.writeStringToFile(sparkSubmit, "#!/bin/sh\nexit 0\n", "UTF-8");
      Assertions.assertTrue(sparkSubmit.setExecutable(true));

      String jobId = exec.submitJob(template);
      Assertions.assertNotNull(jobId);
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .until(() -> exec.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);
    } finally {
      exec.close();
    }
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

  @Test
  public void testInitializeWithSmallJobStatusKeepTimeIsClampedToMinimum()
      throws NoSuchFieldException, IllegalAccessException {
    LocalJobExecutor exec = new LocalJobExecutor();

    exec.initialize(ImmutableMap.of(LocalJobExecutorConfigs.JOB_STATUS_KEEP_TIME_MS, "1"));

    Field field = LocalJobExecutor.class.getDeclaredField("jobStatusKeepTimeInMs");
    field.setAccessible(true);
    long actualValue = (long) field.get(exec);
    Assertions.assertEquals(10L, actualValue);
  }

  @Test
  public void TestInitializeWithLargeJobStatusKeepTimeIsClampedToSameValue()
      throws NoSuchFieldException, IllegalAccessException {
    LocalJobExecutor exec = new LocalJobExecutor();

    exec.initialize(ImmutableMap.of(LocalJobExecutorConfigs.JOB_STATUS_KEEP_TIME_MS, "11"));

    Field field = LocalJobExecutor.class.getDeclaredField("jobStatusKeepTimeInMs");
    field.setAccessible(true);
    long actualValue = (long) field.get(exec);
    Assertions.assertEquals(11L, actualValue);
  }

  private JobTemplate newSleepJobTemplate(String name) throws IOException {
    // The job runs in the directory of its executable, so give each job its own directory.
    File jobDir = new File(workingDir, name);
    Assertions.assertTrue(jobDir.mkdirs());
    File script = new File(jobDir, "sleep.sh");
    // Exec the sleep, so that killing the job process also stops the sleep.
    Files.writeString(script.toPath(), "#!/bin/bash\nexec sleep 600\n");
    Assertions.assertTrue(script.setExecutable(true));

    return ShellJobTemplate.builder()
        .withName(name)
        .withExecutable(script.getAbsolutePath())
        .withArguments(Collections.emptyList())
        .withEnvironments(Collections.emptyMap())
        .withCustomFields(Collections.emptyMap())
        .withScripts(Collections.emptyList())
        .build();
  }
}
