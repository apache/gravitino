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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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

  private static final int DEFAULT_TEST_MAX_BYTES = 1_000_000;

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
  public void testGetJobOutputSuccessfully() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    List<String> stdout = jobExecutor.getJobStdout(jobId, 1000, DEFAULT_TEST_MAX_BYTES);
    Assertions.assertEquals(6, stdout.size());
    Assertions.assertEquals("starting test test job", stdout.get(0));
    Assertions.assertEquals("in common script", stdout.get(1));
    Assertions.assertTrue(stdout.get(2).startsWith("Submitting job with name:"));
    Assertions.assertEquals("value1", stdout.get(3));
    Assertions.assertEquals("success", stdout.get(4));
    Assertions.assertEquals("value3", stdout.get(5));

    // The test script never writes to stderr.
    Assertions.assertEquals(
        Collections.emptyList(), jobExecutor.getJobStderr(jobId, 1000, DEFAULT_TEST_MAX_BYTES));

    // The full output has 6 lines; only the last 3 should be returned when capped.
    Assertions.assertEquals(
        ImmutableList.of("value1", "success", "value3"),
        jobExecutor.getJobStdout(jobId, 3, DEFAULT_TEST_MAX_BYTES));
  }

  @Test
  public void testGetJobOutputForUnknownJobReturnsEmpty() {
    // A job unknown to this executor - whether it never existed here, or its bookkeeping has
    // expired/been lost - reports empty output rather than throwing: the job entity itself may
    // still exist, and querying its output must not turn that into an error.
    Assertions.assertEquals(
        Collections.emptyList(),
        jobExecutor.getJobStdout("no-such-job", 100, DEFAULT_TEST_MAX_BYTES));
    Assertions.assertEquals(
        Collections.emptyList(),
        jobExecutor.getJobStderr("no-such-job", 100, DEFAULT_TEST_MAX_BYTES));
  }

  @Test
  public void testGetJobOutputReturnsEmptyWhenFileDisappearsBetweenExistsCheckAndRead()
      throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // Simulates JobManager#cleanUpStagingDirs racing with this read: the file existed moments
    // ago (the exists() check would pass), but is no longer a readable regular file by the time
    // the actual read is attempted - opening it as a RandomAccessFile throws
    // FileNotFoundException, which must degrade to empty output rather than propagate as a
    // RuntimeException/500.
    File outputLog = new File(workingDir, "output.log");
    Assertions.assertTrue(outputLog.delete());
    Assertions.assertTrue(outputLog.mkdir());

    List<String> stdout = jobExecutor.getJobStdout(jobId, 100, DEFAULT_TEST_MAX_BYTES);
    Assertions.assertEquals(Collections.emptyList(), stdout);
  }

  @Test
  public void testGetJobOutputForQueuedJobReturnsEmpty() throws IOException {
    LocalJobExecutor exec = new LocalJobExecutor();
    exec.initialize(ImmutableMap.of(LocalJobExecutorConfigs.MAX_RUNNING_JOBS, "1"));

    File workingDirA = Files.createTempDirectory("gravitino-test-local-job-executor-a").toFile();
    File workingDirB = Files.createTempDirectory("gravitino-test-local-job-executor-b").toFile();
    try {
      Map<String, String> jobConf =
          ImmutableMap.of(
              "arg1", "value1",
              "arg2", "success",
              "var", "value3");

      // Submit two jobs to a single-threaded executor - the second one stays QUEUED until the
      // first (which sleeps for a few seconds) finishes.
      JobTemplate templateA =
          JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDirA);
      JobTemplate templateB =
          JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDirB);
      exec.submitJob(templateA);
      String jobIdB = exec.submitJob(templateB);

      Assertions.assertEquals(JobHandle.Status.QUEUED, exec.getJobStatus(jobIdB));
      Assertions.assertEquals(
          Collections.emptyList(), exec.getJobStdout(jobIdB, 100, DEFAULT_TEST_MAX_BYTES));
      Assertions.assertEquals(
          Collections.emptyList(), exec.getJobStderr(jobIdB, 100, DEFAULT_TEST_MAX_BYTES));

      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .until(() -> exec.getJobStatus(jobIdB) == JobHandle.Status.SUCCEEDED);
    } finally {
      exec.close();
      FileUtils.deleteDirectory(workingDirA);
      FileUtils.deleteDirectory(workingDirB);
    }
  }

  @Test
  public void testGetJobOutputWithOversizedSingleLineIsBoundedByMaxBytes() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // Overwrite the captured output with a single line far larger than the byte window, with no
    // trailing newline - the pathological case a byte-bounded tail read must stay safe against
    // (a naive line-oriented reverse reader can degrade badly on content shaped like this).
    int maxBytes = 1024;
    String oversizedLine = StringUtils.repeat('x', maxBytes * 4);
    FileUtils.writeStringToFile(new File(workingDir, "output.log"), oversizedLine, "UTF-8");

    List<String> stdout = jobExecutor.getJobStdout(jobId, 1000, maxBytes);
    Assertions.assertEquals(1, stdout.size());
    Assertions.assertTrue(stdout.get(0).length() <= maxBytes);
    Assertions.assertTrue(oversizedLine.endsWith(stdout.get(0)));
  }

  @Test
  public void testGetJobOutputWithOversizedSingleLineEndingInNewlineIsTruncatedNotEmpty()
      throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // Same oversized single line as above, but terminated with a newline - the shape almost
    // every real shell command (echo, printf '...\n') actually produces. The trailing newline is
    // then the only '\n' in the window, and must not be mistaken for a boundary to a subsequent
    // line - the truncated line content must still come back, not an empty list.
    int maxBytes = 1024;
    String oversizedLine = StringUtils.repeat('x', maxBytes * 4);
    FileUtils.writeStringToFile(new File(workingDir, "output.log"), oversizedLine + "\n", "UTF-8");

    List<String> stdout = jobExecutor.getJobStdout(jobId, 1000, maxBytes);
    Assertions.assertEquals(1, stdout.size());
    Assertions.assertFalse(stdout.get(0).isEmpty());
    Assertions.assertTrue(stdout.get(0).length() <= maxBytes);
    Assertions.assertTrue(oversizedLine.endsWith(stdout.get(0)));
  }

  @Test
  public void testGetJobOutputWithOrdinaryLinesFollowedByOversizedFinalLine() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // A few ordinary lines followed by a newline-terminated final line that alone exceeds the
    // byte window - the window falls entirely within that last line, so its trailing newline is
    // again the only one visible, and the truncated tail of that line must still be returned.
    int maxBytes = 1024;
    String oversizedLine = StringUtils.repeat('y', maxBytes * 4);
    String content =
        "error: something failed\nstack frame 1\nstack frame 2\n" + oversizedLine + "\n";
    FileUtils.writeStringToFile(new File(workingDir, "output.log"), content, "UTF-8");

    List<String> stdout = jobExecutor.getJobStdout(jobId, 1000, maxBytes);
    Assertions.assertEquals(1, stdout.size());
    Assertions.assertFalse(stdout.get(0).isEmpty());
    Assertions.assertTrue(stdout.get(0).length() <= maxBytes);
    Assertions.assertTrue(oversizedLine.endsWith(stdout.get(0)));
  }

  @Test
  public void testGetJobOutputWindowSmallerThanRequestedLines() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // Many short lines whose total size exceeds the byte window - only the lines that fit in
    // the window should come back, even though maxLines asks for far more than that.
    int lineCount = 2000;
    StringBuilder content = new StringBuilder();
    for (int i = 0; i < lineCount; i++) {
      content.append("line").append(i).append('\n');
    }
    FileUtils.writeStringToFile(new File(workingDir, "output.log"), content.toString(), "UTF-8");

    int maxBytes = 100;
    List<String> stdout = jobExecutor.getJobStdout(jobId, lineCount, maxBytes);
    Assertions.assertFalse(stdout.isEmpty());
    Assertions.assertTrue(stdout.size() < lineCount);
    // It's a tail read, so the very last written line must always be present.
    Assertions.assertEquals("line" + (lineCount - 1), stdout.get(stdout.size() - 1));
  }

  @Test
  public void testGetJobOutputStripsCrlfLineEndings() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // Windows-style line endings must not leave a trailing '\r' on the returned lines.
    FileUtils.writeStringToFile(
        new File(workingDir, "output.log"), "line1\r\nline2\r\nline3\r\n", "UTF-8");

    List<String> stdout = jobExecutor.getJobStdout(jobId, 100, DEFAULT_TEST_MAX_BYTES);
    Assertions.assertEquals(ImmutableList.of("line1", "line2", "line3"), stdout);
  }

  @Test
  public void testGetJobOutputKeepsAllLinesWhenWindowAlignsOnLineBoundary() throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // "line1\n" is exactly 6 bytes, so a maxBytes of 12 makes the window start exactly at the
    // beginning of "line2" - a genuine line boundary, not a partial line. Both "line2" and
    // "line3" must be returned, not just the last one.
    FileUtils.writeStringToFile(
        new File(workingDir, "output.log"), "line1\nline2\nline3\n", "UTF-8");

    List<String> stdout = jobExecutor.getJobStdout(jobId, 100, 12);
    Assertions.assertEquals(ImmutableList.of("line2", "line3"), stdout);
  }

  @Test
  public void testGetJobOutputAvoidsCorruptingMultiByteUtf8CharacterAtWindowStart()
      throws IOException {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "arg1", "value1",
            "arg2", "success",
            "var", "value3");

    JobTemplate template =
        JobManager.createRuntimeJobTemplate(jobTemplateEntity, jobConf, workingDir);

    String jobId = jobExecutor.submitJob(template);
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> jobExecutor.getJobStatus(jobId) == JobHandle.Status.SUCCEEDED);

    // "AAAAA" + "中" (3-byte UTF-8: 0xE4 0xB8 0xAD) + "BBBBB", with no newlines at all. With
    // maxBytes=7 the computed window start lands on the second byte of the multi-byte character
    // - the read must skip forward to the next character boundary rather than emitting a
    // replacement character for the split-up bytes.
    ByteArrayOutputStream content = new ByteArrayOutputStream();
    content.write("AAAAA".getBytes(StandardCharsets.UTF_8));
    content.write(new byte[] {(byte) 0xE4, (byte) 0xB8, (byte) 0xAD});
    content.write("BBBBB".getBytes(StandardCharsets.UTF_8));
    FileUtils.writeByteArrayToFile(new File(workingDir, "output.log"), content.toByteArray());

    List<String> stdout = jobExecutor.getJobStdout(jobId, 100, 7);
    Assertions.assertEquals(ImmutableList.of("BBBBB"), stdout);
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
