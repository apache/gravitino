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
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class JobIT extends BaseIT {

  private static final String METALAKE_NAME = GravitinoITUtils.genRandomName("job_it_metalake");

  private File testStagingDir;
  private String testEntryScriptPath;
  private String testLibScriptPath;
  private ShellJobTemplate.Builder builder;
  private GravitinoMetalake metalake;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    testStagingDir = Files.createTempDirectory("test_staging_dir").toFile();
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
            "3000");
    registerCustomConfigs(configs);
    super.startIntegrationTest();
  }

  @AfterAll
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testStagingDir);
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
    Assertions.assertEquals(2, registeredTemplates.size());
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
    Assertions.assertEquals(2, registeredTemplates.size());
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
    Assertions.assertEquals(1, registeredTemplates.size());
    Assertions.assertTrue(registeredTemplates.contains(template2));

    // Test deleting a non-existent job template
    Assertions.assertFalse(metalake.deleteJobTemplate(template1.name()));

    // Delete the second job template
    Assertions.assertTrue(metalake.deleteJobTemplate(template2.name()));

    // Verify the second job template is deleted
    Assertions.assertThrows(
        NoSuchJobTemplateException.class, () -> metalake.getJobTemplate(template2.name()));

    // Verify the list of job templates is empty after deleting both
    registeredTemplates = metalake.listJobTemplates();
    Assertions.assertTrue(registeredTemplates.isEmpty());
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
