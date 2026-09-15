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
package org.apache.gravitino.client.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobIT extends BaseIT {

  private static final String METALAKE_NAME = GravitinoITUtils.genRandomName("job_it_metalake");

  private static final long STATUS_PULL_INTERVAL_IN_MS = 3000L;

  // Finished jobs, and active jobs not updated, for this long are cleaned up. The cleanup runs
  // every tenth of it.
  private static final long JOB_KEEP_TIME_IN_MS = 60_000L;

  // A job execution id that no job executor instance of this server owns, as if the job was
  // submitted by the local job executor of another Gravitino server sharing the metadata store.
  private static final String OTHER_SERVER_EXECUTION_ID_PREFIX = "local-job-otherserver-";

  private File testStagingDir;
  private File testSparkHome;
  private String testEntryScriptPath;
  private String testLibScriptPath;
  private ShellJobTemplate.Builder builder;
  private GravitinoMetalake metalake;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    testStagingDir = Files.createTempDirectory("test_staging_dir").toFile();
    // A Spark home without bin/spark-submit, so Spark jobs cannot be launched. The configuration
    // takes precedence over the SPARK_HOME environment variable, keeping the test deterministic.
    testSparkHome = Files.createTempDirectory("test_spark_home").toFile();
    testEntryScriptPath = generateTestEntryScript();
    testLibScriptPath = generateTestLibScript();

    builder =
        ShellJobTemplate.builder()
            .withComment("Test shell job template")
            .withExecutable(testEntryScriptPath)
            .withArguments(Lists.newArrayList("{{arg1}}", "{{arg2}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "{{env_var}}"))
            .withScripts(Lists.newArrayList(testLibScriptPath))
            .withCustomFields(Collections.emptyMap());

    Map<String, String> configs =
        ImmutableMap.of(
            "gravitino.job.stagingDir",
            testStagingDir.getAbsolutePath(),
            "gravitino.job.statusPullIntervalInMs",
            String.valueOf(STATUS_PULL_INTERVAL_IN_MS),
            "gravitino.job.stagingDirKeepTimeInMs",
            String.valueOf(JOB_KEEP_TIME_IN_MS),
            "gravitino.jobExecutor.local.sparkHome",
            testSparkHome.getAbsolutePath());
    registerCustomConfigs(configs);
    super.startIntegrationTest();
  }

  @AfterAll
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testStagingDir);
    FileUtils.deleteDirectory(testSparkHome);
  }

  @BeforeEach
  public void setUp() {
    // Create a metalake for testing jobs
    metalake =
        client.createMetalake(METALAKE_NAME, "metalake test for job", Collections.emptyMap());
  }

  @AfterEach
  public void cleanUp() {
    // Drop the metalake after each test
    client.dropMetalake(METALAKE_NAME, true);
  }

  @Test
  public void testRegisterAndListJobTemplates() {
    JobTemplate template1 = builder.withName("test_1").build();
    JobTemplate template2 = builder.withName("test_2").build();

    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template1));
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template2));

    List<JobTemplate> registeredTemplates = metalake.listJobTemplates();
    Assertions.assertTrue(registeredTemplates.contains(template1));
    Assertions.assertTrue(registeredTemplates.contains(template2));

    // Test register duplicated job template
    Assertions.assertThrows(
        JobTemplateAlreadyExistsException.class, () -> metalake.registerJobTemplate(template1));
  }

  @Test
  public void testRegisterAndGetJobTemplate() {
    JobTemplate template = builder.withName("test_get").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    JobTemplate retrievedTemplate = metalake.getJobTemplate(template.name());
    Assertions.assertEquals(template, retrievedTemplate);

    // Test get non-existent job template
    Assertions.assertThrows(
        NoSuchJobTemplateException.class, () -> metalake.getJobTemplate("non_existent_template"));
  }

  @Test
  public void testRegisterAndDeleteJobTemplate() {
    JobTemplate template1 = builder.withName("test_1").build();
    JobTemplate template2 = builder.withName("test_2").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template1));
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template2));

    List<JobTemplate> registeredTemplates = metalake.listJobTemplates();
    Assertions.assertTrue(registeredTemplates.contains(template1));
    Assertions.assertTrue(registeredTemplates.contains(template2));

    JobTemplate result1 = metalake.getJobTemplate(template1.name());
    JobTemplate result2 = metalake.getJobTemplate(template2.name());
    Assertions.assertEquals(template1, result1);
    Assertions.assertEquals(template2, result2);

    // Delete the first job template
    Assertions.assertTrue(metalake.deleteJobTemplate(template1.name()));
    // Verify the first job template is deleted
    Assertions.assertThrows(
        NoSuchJobTemplateException.class, () -> metalake.getJobTemplate(template1.name()));
    // Verify the second job template still exists
    JobTemplate remainingTemplate = metalake.getJobTemplate(template2.name());
    Assertions.assertEquals(template2, remainingTemplate);

    // Verify the list of job templates after deletion
    registeredTemplates = metalake.listJobTemplates();
    Assertions.assertTrue(registeredTemplates.contains(template2));

    // Test deleting a non-existent job template
    Assertions.assertFalse(metalake.deleteJobTemplate(template1.name()));

    // Delete the second job template
    Assertions.assertTrue(metalake.deleteJobTemplate(template2.name()));

    // Verify the second job template is deleted
    Assertions.assertThrows(
        NoSuchJobTemplateException.class, () -> metalake.getJobTemplate(template2.name()));
  }

  @Test
  public void testRegisterAndAlterJobTemplate() {
    ShellJobTemplate template = builder.withName("test_alter").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    // Rename the job template
    JobTemplate renamedTemplate =
        metalake.alterJobTemplate(template.name(), JobTemplateChange.rename("renamed_test_alter"));

    // Verify the job template is renamed
    Assertions.assertEquals("renamed_test_alter", renamedTemplate.name());
    Assertions.assertEquals(template.comment(), renamedTemplate.comment());
    Assertions.assertEquals(template.executable(), renamedTemplate.executable());
    Assertions.assertEquals(template.arguments(), renamedTemplate.arguments());
    Assertions.assertEquals(template.environments(), renamedTemplate.environments());
    Assertions.assertEquals(template.customFields(), renamedTemplate.customFields());
    Assertions.assertEquals(template.scripts(), ((ShellJobTemplate) renamedTemplate).scripts());

    JobTemplate fetchedTemplate = metalake.getJobTemplate(renamedTemplate.name());
    Assertions.assertEquals(renamedTemplate, fetchedTemplate);

    // Update the job template's comment
    JobTemplate updatedTemplate =
        metalake.alterJobTemplate(
            renamedTemplate.name(), JobTemplateChange.updateComment("Updated comment"));

    // Verify the job template comment is updated
    Assertions.assertEquals("Updated comment", updatedTemplate.comment());
    Assertions.assertEquals(renamedTemplate.name(), updatedTemplate.name());
    Assertions.assertEquals(template.executable(), updatedTemplate.executable());
    Assertions.assertEquals(template.arguments(), updatedTemplate.arguments());
    Assertions.assertEquals(template.environments(), updatedTemplate.environments());
    Assertions.assertEquals(template.customFields(), updatedTemplate.customFields());
    Assertions.assertEquals(template.scripts(), ((ShellJobTemplate) updatedTemplate).scripts());

    // Fetch the updated template and verify
    JobTemplate fetchedUpdatedTemplate = metalake.getJobTemplate(updatedTemplate.name());
    Assertions.assertEquals(updatedTemplate, fetchedUpdatedTemplate);

    // Update the job template's executable, arguments, and environments
    JobTemplateChange.ShellTemplateUpdate update =
        JobTemplateChange.ShellTemplateUpdate.builder()
            .withNewExecutable("/new/path/to/executable.sh")
            .withNewArguments(Lists.newArrayList("newArg1", "newArg2"))
            .withNewEnvironments(ImmutableMap.of("NEW_ENV", "newValue"))
            .build();

    JobTemplate modifiedTemplate =
        metalake.alterJobTemplate(updatedTemplate.name(), JobTemplateChange.updateTemplate(update));

    // Verify the job template fields are updated
    Assertions.assertEquals(update.getNewExecutable(), modifiedTemplate.executable());
    Assertions.assertEquals(update.getNewArguments(), modifiedTemplate.arguments());
    Assertions.assertEquals(update.getNewEnvironments(), modifiedTemplate.environments());
    Assertions.assertEquals(updatedTemplate.customFields(), modifiedTemplate.customFields());
    Assertions.assertEquals(
        ((ShellJobTemplate) updatedTemplate).scripts(),
        ((ShellJobTemplate) modifiedTemplate).scripts());

    // Fetch the modified template and verify
    JobTemplate fetchedModifiedTemplate = metalake.getJobTemplate(modifiedTemplate.name());
    Assertions.assertEquals(modifiedTemplate, fetchedModifiedTemplate);

    // Update the job template's custom fields and scripts
    JobTemplateChange.ShellTemplateUpdate update1 =
        JobTemplateChange.ShellTemplateUpdate.builder()
            .withNewCustomFields(ImmutableMap.of("customKey", "customValue"))
            .withNewScripts(Lists.newArrayList(testLibScriptPath, "/new/path/to/script.sh"))
            .build();

    JobTemplate finalTemplate =
        metalake.alterJobTemplate(
            modifiedTemplate.name(), JobTemplateChange.updateTemplate(update1));

    // Verify the job template fields are updated
    Assertions.assertEquals(modifiedTemplate.executable(), finalTemplate.executable());
    Assertions.assertEquals(modifiedTemplate.arguments(), finalTemplate.arguments());
    Assertions.assertEquals(modifiedTemplate.environments(), finalTemplate.environments());
    Assertions.assertEquals(update1.getNewCustomFields(), finalTemplate.customFields());
    Assertions.assertEquals(update1.getNewScripts(), ((ShellJobTemplate) finalTemplate).scripts());

    // Fetch the final template and verify
    JobTemplate fetchedFinalTemplate = metalake.getJobTemplate(finalTemplate.name());
    Assertions.assertEquals(finalTemplate, fetchedFinalTemplate);

    // Test altering a non-existent job template
    Assertions.assertThrows(
        NoSuchJobTemplateException.class,
        () ->
            metalake.alterJobTemplate(
                "non_existent_template", JobTemplateChange.rename("new_name")));

    // Test altering with wrong change type
    JobTemplateChange.SparkTemplateUpdate wrongUpdate =
        JobTemplateChange.SparkTemplateUpdate.builder()
            .withNewClassName("com.example.Main")
            .build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            metalake.alterJobTemplate(
                finalTemplate.name(), JobTemplateChange.updateTemplate(wrongUpdate)));
  }

  @Test
  public void testRunAndListJobs() {
    JobTemplate template = builder.withName("test_run").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    // Submit a job with success status
    JobHandle jobHandle1 =
        metalake.runJob(
            template.name(),
            ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2"));
    Assertions.assertEquals(JobHandle.Status.QUEUED, jobHandle1.jobStatus());
    Assertions.assertEquals(template.name(), jobHandle1.jobTemplateName());

    JobHandle jobHandle2 =
        metalake.runJob(
            template.name(),
            ImmutableMap.of("arg1", "value3", "arg2", "success", "env_var", "value4"));
    Assertions.assertEquals(JobHandle.Status.QUEUED, jobHandle2.jobStatus());
    Assertions.assertEquals(template.name(), jobHandle2.jobTemplateName());

    List<JobHandle> jobs = metalake.listJobs(template.name());
    Assertions.assertEquals(2, jobs.size());
    List<String> resultJobIds = jobs.stream().map(JobHandle::jobId).collect(Collectors.toList());
    Assertions.assertTrue(resultJobIds.contains(jobHandle1.jobId()));
    Assertions.assertTrue(resultJobIds.contains(jobHandle2.jobId()));

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(
            () -> {
              JobHandle updatedJob1 = metalake.getJob(jobHandle1.jobId());
              return updatedJob1.jobStatus() == JobHandle.Status.SUCCEEDED;
            });

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(
            () -> {
              JobHandle updatedJob2 = metalake.getJob(jobHandle2.jobId());
              return updatedJob2.jobStatus() == JobHandle.Status.SUCCEEDED;
            });

    List<JobHandle> updatedJobs = metalake.listJobs(template.name());
    Assertions.assertEquals(2, updatedJobs.size());
    Set<JobHandle.Status> jobStatuses =
        updatedJobs.stream().map(JobHandle::jobStatus).collect(Collectors.toSet());
    Assertions.assertEquals(1, jobStatuses.size());
    Assertions.assertTrue(jobStatuses.contains(JobHandle.Status.SUCCEEDED));
  }

  @Test
  public void testRunSparkJobRejectedWhenSparkIsNotAvailable() {
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("test_run_spark_without_spark_submit")
            .withComment("Test spark job template")
            .withExecutable(testEntryScriptPath)
            .withClassName("org.apache.gravitino.test.SparkJob")
            .build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    // The run request is rejected with the reason instead of being queued and failing later.
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.runJob(template.name(), Collections.emptyMap()));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "spark-submit is not found or not executable: "
                    + testSparkHome.getAbsolutePath()
                    + "/bin/spark-submit"),
        e.getMessage());

    // No job is created, and the staging directory of the rejected job is removed.
    Assertions.assertTrue(metalake.listJobs(template.name()).isEmpty());
    String[] jobStagingDirs =
        new File(testStagingDir, METALAKE_NAME + File.separator + template.name()).list();
    Assertions.assertTrue(jobStagingDirs == null || jobStagingDirs.length == 0);

    // Shell jobs are not affected by the missing Spark installation.
    JobTemplate shellTemplate = builder.withName("test_run_shell_without_spark_submit").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(shellTemplate));
    JobHandle jobHandle =
        metalake.runJob(
            shellTemplate.name(),
            ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2"));
    Assertions.assertEquals(JobHandle.Status.QUEUED, jobHandle.jobStatus());
    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(() -> metalake.getJob(jobHandle.jobId()).jobStatus() == JobHandle.Status.SUCCEEDED);
  }

  @Test
  public void testRunAndGetJob() {
    JobTemplate template = builder.withName("test_run_get").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    // Submit a job with success status
    JobHandle jobHandle =
        metalake.runJob(
            template.name(),
            ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2"));
    Assertions.assertEquals(JobHandle.Status.QUEUED, jobHandle.jobStatus());
    Assertions.assertEquals(template.name(), jobHandle.jobTemplateName());

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(
            () -> {
              JobHandle updatedJob = metalake.getJob(jobHandle.jobId());
              return updatedJob.jobStatus() == JobHandle.Status.SUCCEEDED;
            });

    JobHandle retrievedJob = metalake.getJob(jobHandle.jobId());
    Assertions.assertEquals(jobHandle.jobId(), retrievedJob.jobId());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, retrievedJob.jobStatus());

    // Test run a failed job
    JobHandle failedJobHandle =
        metalake.runJob(
            template.name(),
            ImmutableMap.of("arg1", "value1", "arg2", "fail", "env_var", "value2"));
    Assertions.assertEquals(JobHandle.Status.QUEUED, failedJobHandle.jobStatus());

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(
            () -> {
              JobHandle updatedFailedJob = metalake.getJob(failedJobHandle.jobId());
              return updatedFailedJob.jobStatus() == JobHandle.Status.FAILED;
            });

    JobHandle retrievedFailedJob = metalake.getJob(failedJobHandle.jobId());
    Assertions.assertEquals(failedJobHandle.jobId(), retrievedFailedJob.jobId());
    Assertions.assertEquals(JobHandle.Status.FAILED, retrievedFailedJob.jobStatus());

    // Test get a non-existent job
    Assertions.assertThrows(NoSuchJobException.class, () -> metalake.getJob("non_existent_job_id"));
  }

  @Test
  public void testRunAndCancelJob() {
    JobTemplate template = builder.withName("test_run_cancel").build();
    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    // Submit a job with success status
    JobHandle jobHandle =
        metalake.runJob(
            template.name(),
            ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2"));
    Assertions.assertEquals(JobHandle.Status.QUEUED, jobHandle.jobStatus());
    Assertions.assertEquals(template.name(), jobHandle.jobTemplateName());

    // Cancel the job
    metalake.cancelJob(jobHandle.jobId());

    Awaitility.await()
        .atMost(3, TimeUnit.MINUTES)
        .until(
            () -> {
              JobHandle updatedJob = metalake.getJob(jobHandle.jobId());
              return updatedJob.jobStatus() == JobHandle.Status.CANCELLED;
            });

    JobHandle retrievedJob = metalake.getJob(jobHandle.jobId());
    Assertions.assertEquals(jobHandle.jobId(), retrievedJob.jobId());
    Assertions.assertEquals(JobHandle.Status.CANCELLED, retrievedJob.jobStatus());

    // Test cancel a non-existent job
    Assertions.assertThrows(
        NoSuchJobException.class, () -> metalake.cancelJob("non_existent_job_id"));
  }

  @Test
  public void testJobOwnedByAnotherServerIsNotFailed() throws Exception {
    Assumptions.assumeTrue(
        ITUtils.EMBEDDED_TEST_MODE.equals(testMode),
        "Simulating another server needs direct access to the server's metadata store");
    JobTemplate template = builder.withName("test_other_server_job").build();
    metalake.registerJobTemplate(template);

    // Another server's job can't be found in this server's job executor, but this server must not
    // mark it as FAILED when pulling job statuses.
    JobEntity job = insertOtherServerJob(template.name(), JobHandle.Status.STARTED, Instant.now());
    Awaitility.await()
        .during(STATUS_PULL_INTERVAL_IN_MS * 2, TimeUnit.MILLISECONDS)
        .atMost(STATUS_PULL_INTERVAL_IN_MS * 2 + 1000, TimeUnit.MILLISECONDS)
        .until(() -> metalake.getJob(job.name()).jobStatus() == JobHandle.Status.STARTED);
  }

  @Test
  public void testCancelJobOwnedByAnotherServer() throws Exception {
    Assumptions.assumeTrue(
        ITUtils.EMBEDDED_TEST_MODE.equals(testMode),
        "Simulating another server needs direct access to the server's metadata store");
    JobTemplate template = builder.withName("test_other_server_cancel").build();
    metalake.registerJobTemplate(template);
    JobEntity job = insertOtherServerJob(template.name(), JobHandle.Status.STARTED, Instant.now());

    // This server can't cancel another server's job, so it marks the job as CANCELLING for its
    // owner to cancel, instead of failing the request.
    JobHandle cancellingJob = metalake.cancelJob(job.name());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, cancellingJob.jobStatus());
    Awaitility.await()
        .during(STATUS_PULL_INTERVAL_IN_MS * 2, TimeUnit.MILLISECONDS)
        .atMost(STATUS_PULL_INTERVAL_IN_MS * 2 + 1000, TimeUnit.MILLISECONDS)
        .until(() -> metalake.getJob(job.name()).jobStatus() == JobHandle.Status.CANCELLING);
  }

  @Test
  public void testStaleActiveJobIsMarkedFailed() throws Exception {
    Assumptions.assumeTrue(
        ITUtils.EMBEDDED_TEST_MODE.equals(testMode),
        "Simulating another server needs direct access to the server's metadata store");
    JobTemplate template = builder.withName("test_stale_job").build();
    metalake.registerJobTemplate(template);

    // The job was left behind by a server that exited long ago, so nobody updates it anymore.
    JobEntity job =
        insertOtherServerJob(
            template.name(), JobHandle.Status.STARTED, Instant.now().minus(Duration.ofDays(30)));
    Assertions.assertEquals(JobHandle.Status.STARTED, metalake.getJob(job.name()).jobStatus());

    // The cleanup marks the job as FAILED, as it has not been updated for longer than the keep
    // time. The failed job is kept for another keep time before being removed.
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> metalake.getJob(job.name()).jobStatus() == JobHandle.Status.FAILED);
  }

  private JobEntity insertOtherServerJob(
      String templateName, JobHandle.Status status, Instant createTime) throws IOException {
    long jobId = GravitinoEnv.getInstance().idGenerator().nextId();
    JobEntity job =
        JobEntity.builder()
            .withId(jobId)
            .withJobExecutionId(OTHER_SERVER_EXECUTION_ID_PREFIX + UUID.randomUUID())
            .withJobTemplateName(templateName)
            .withStatus(status)
            .withNamespace(NamespaceUtil.ofJob(METALAKE_NAME))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(createTime).build())
            .withFinishedAt(0L)
            .build();
    GravitinoEnv.getInstance().entityStore().put(job, false /* overwrite */);
    return job;
  }

  private String generateTestEntryScript() {
    String content =
        "#!/bin/bash\n"
            + "echo \"starting test test job\"\n\n"
            + "bin=\"$(dirname \"${BASH_SOURCE-$0}\")\"\n"
            + "bin=\"$(cd \"${bin}\">/dev/null; pwd)\"\n\n"
            + ". \"${bin}/common.sh\"\n\n"
            + "sleep 3\n\n"
            + "JOB_NAME=\"test_job-$(date +%s)-$1\"\n\n"
            + "echo \"Submitting job with name: $JOB_NAME\"\n\n"
            + "echo \"$1\"\n\n"
            + "echo \"$2\"\n\n"
            + "echo \"$ENV_VAR\"\n\n"
            + "if [[ \"$2\" == \"success\" ]]; then\n"
            + "  exit 0\n"
            + "elif [[ \"$2\" == \"fail\" ]]; then\n"
            + "  exit 1\n"
            + "else\n"
            + "  exit 2\n"
            + "fi\n";

    // save the script to a file
    try {
      File scriptFile = new File(testStagingDir, "test-job.sh");
      Files.writeString(scriptFile.toPath(), content);
      scriptFile.setExecutable(true);
      return scriptFile.getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test entry script", e);
    }
  }

  private String generateTestLibScript() {
    String content = "#!/bin/bash\necho \"in common script\"\n";

    // save the script to a file
    try {
      File scriptFile = new File(testStagingDir, "common.sh");
      Files.writeString(scriptFile.toPath(), content);
      scriptFile.setExecutable(true);
      return scriptFile.getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test lib script", e);
    }
  }
}
