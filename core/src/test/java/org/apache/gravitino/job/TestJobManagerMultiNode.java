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
package org.apache.gravitino.job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.local.LocalJobExecutor;
import org.apache.gravitino.job.local.LocalJobExecutorConfigs;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Tests the job system in a multi-node deployment: two Gravitino servers, each with its own {@link
 * JobManager} and {@link LocalJobExecutor}, share the same relational metadata store. The status
 * pull and the cleanup are triggered manually, so that the test controls which node runs them when.
 */
public class TestJobManagerMultiNode extends TestJDBCBackend {

  private static final String METALAKE = "metalake_job_multi_node";

  private static final String TEMPLATE = "sleep_job";

  private static final String ECHO_TEMPLATE = "echo_job";

  // Active jobs not updated for this long are expired, and finished jobs are cleaned up after it.
  // The tests move the job timestamps back instead of waiting for it to elapse.
  private static final long JOB_KEEP_TIME_IN_MS = TimeUnit.HOURS.toMillis(1);

  private File testDir;

  private LocalJobExecutor executorA;

  private LocalJobExecutor executorB;

  private JobManager nodeA;

  private JobManager nodeB;

  private EntityStore entityStore;

  @BeforeEach
  public void setUpNodes() throws Exception {
    testDir = Files.createTempDirectory("gravitino-test-job-multi-node").toFile();

    // Both nodes share the same staging directory, like a deployment on shared storage.
    Config config = newConfig(new File(testDir, "staging"));
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    // Both nodes share the same metadata store, backed by the relational backend under test.
    RelationalEntityStore relationalEntityStore = new RelationalEntityStore();
    FieldUtils.writeField(relationalEntityStore, "backend", backend, true);
    FieldUtils.writeField(relationalEntityStore, "cache", new NoOpsCache(config), true);
    entityStore = relationalEntityStore;

    createAndInsertMakeLake(METALAKE);
    backend.insert(newSleepJobTemplateEntity(), false);
    backend.insert(newEchoJobTemplateEntity(), false);

    executorA = newLocalJobExecutor(config);
    executorB = newLocalJobExecutor(config);
    nodeA = newJobManager(config, entityStore, executorA);
    nodeB = newJobManager(config, entityStore, executorB);
  }

  @AfterEach
  public void tearDownNodes() throws IOException {
    // Closing a node also kills the job processes it runs.
    if (nodeA != null) {
      nodeA.close();
    }
    if (nodeB != null) {
      nodeB.close();
    }
    FileUtils.deleteDirectory(testDir);
  }

  @TestTemplate
  public void testJobIsOnlyTrackedByItsOwnerNode() throws IOException {
    JobEntity job = nodeA.runJob(METALAKE, TEMPLATE, ImmutableMap.of("seconds", "0"));
    Assertions.assertTrue(executorA.ownsJob(job.jobExecutionId()));
    Assertions.assertFalse(executorB.ownsJob(job.jobExecutionId()));
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.SUCCEEDED);

    // Before the fix, node B couldn't find the job in its own executor and marked it as FAILED.
    nodeB.pullAndUpdateJobStatus();
    JobEntity afterNodeBPull = getJob(job.name());
    Assertions.assertEquals(JobHandle.Status.QUEUED, afterNodeBPull.status());
    Assertions.assertEquals(job.auditInfo(), afterNodeBPull.auditInfo());

    nodeA.pullAndUpdateJobStatus();
    JobEntity afterNodeAPull = getJob(job.name());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, afterNodeAPull.status());
    Assertions.assertTrue(afterNodeAPull.finishedAt() > 0);
  }

  @TestTemplate
  public void testCancelJobFromAnotherNode() throws IOException {
    JobEntity job = runLongJobOnNodeA();

    // Node B can't cancel the job itself, so it only marks the job as CANCELLING.
    JobEntity cancelling = nodeB.cancelJob(METALAKE, job.name());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, cancelling.status());
    Assertions.assertEquals(JobHandle.Status.STARTED, executorA.getJobStatus(job.jobExecutionId()));

    // Node A cancels the job when it pulls the job status next time.
    nodeA.pullAndUpdateJobStatus();
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.CANCELLED);
    // The first pull may already see the job as CANCELLED if the process exits quickly.
    Assertions.assertTrue(
        EnumSet.of(JobHandle.Status.CANCELLING, JobHandle.Status.CANCELLED)
            .contains(getJob(job.name()).status()));

    nodeA.pullAndUpdateJobStatus();
    JobEntity cancelled = getJob(job.name());
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelled.status());
    Assertions.assertTrue(cancelled.finishedAt() > 0);
  }

  @TestTemplate
  public void testJobsLeftByExitedNodeAreExpiredByAnotherNode() throws Exception {
    JobEntity startedJob = runLongJobOnNodeA();
    // Cancel this job from node B, node A exits before cancelling it.
    JobEntity cancellingJob = nodeA.runJob(METALAKE, TEMPLATE, ImmutableMap.of("seconds", "600"));
    nodeB.cancelJob(METALAKE, cancellingJob.name());

    // Node A exits, and nobody updates its jobs anymore.
    nodeA.close();
    nodeA = null;

    // Neither the status pull nor the cleanup of node B touches the jobs before they expire.
    nodeB.pullAndUpdateJobStatus();
    nodeB.cleanUpStagingDirs();
    Assertions.assertEquals(JobHandle.Status.STARTED, getJob(startedJob.name()).status());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, getJob(cancellingJob.name()).status());

    // Once the jobs have not been updated for the keep time, node B marks them as finished.
    moveJobTimestampsBack(startedJob.name());
    moveJobTimestampsBack(cancellingJob.name());
    nodeB.cleanUpStagingDirs();
    JobEntity failedJob = getJob(startedJob.name());
    JobEntity cancelledJob = getJob(cancellingJob.name());
    Assertions.assertEquals(JobHandle.Status.FAILED, failedJob.status());
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelledJob.status());

    // The finished jobs are kept for another keep time, and then cleaned up.
    nodeB.cleanUpStagingDirs();
    Assertions.assertTrue(jobExists(startedJob.name()));
    Assertions.assertTrue(jobExists(cancellingJob.name()));

    moveJobTimestampsBack(startedJob.name());
    moveJobTimestampsBack(cancellingJob.name());
    nodeB.cleanUpStagingDirs();
    Assertions.assertFalse(jobExists(startedJob.name()));
    Assertions.assertFalse(jobExists(cancellingJob.name()));
  }

  @TestTemplate
  public void testGetJobOutputFromAnotherNode() throws IOException {
    JobEntity job = runEchoJobOnNodeA("a");

    // Node B didn't run the job, it reads the output from the shared staging directory.
    JobEntity jobWithOutput = nodeB.getJob(METALAKE, job.name(), true);
    Assertions.assertEquals(ImmutableList.of("hello a"), jobWithOutput.stdout());
    Assertions.assertEquals(ImmutableList.of("oops a"), jobWithOutput.stderr());

    // The output doesn't depend on the node that ran the job being alive.
    nodeA.close();
    nodeA = null;
    Assertions.assertEquals(
        ImmutableList.of("hello a"), nodeB.getJob(METALAKE, job.name(), true).stdout());
  }

  @TestTemplate
  public void testGetJobOutputAfterTemplateRenamed() throws IOException {
    JobEntity job = runEchoJobOnNodeA("b");

    String newName = ECHO_TEMPLATE + "_renamed";
    nodeA.alterJobTemplate(METALAKE, ECHO_TEMPLATE, JobTemplateChange.rename(newName));

    // The job reports the new template name, which its staging directory doesn't depend on.
    JobEntity jobWithOutput = nodeB.getJob(METALAKE, job.name(), true);
    Assertions.assertEquals(newName, jobWithOutput.jobTemplateName());
    Assertions.assertEquals(ImmutableList.of("hello b"), jobWithOutput.stdout());
  }

  @TestTemplate
  public void testGetJobOutputOfTemplateNamedWithSpecialCharacters() throws IOException {
    // Template names are not restricted, and must not affect running the job or reading its output.
    for (String templateName : ImmutableList.of("etl job \"v2\" 中文 #1", "team/etl")) {
      backend.insert(
          newScriptJobTemplateEntity(
              templateName, "echo \"hello $1\"\necho \"oops $1\" >&2\n", "{{name}}"),
          false);

      JobEntity job = nodeA.runJob(METALAKE, templateName, ImmutableMap.of("name", "d"));
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.SUCCEEDED);

      JobEntity jobWithOutput = nodeB.getJob(METALAKE, job.name(), true);
      Assertions.assertEquals(ImmutableList.of("hello d"), jobWithOutput.stdout(), templateName);
      Assertions.assertEquals(ImmutableList.of("oops d"), jobWithOutput.stderr(), templateName);
    }
  }

  @TestTemplate
  public void testGetJobOutputDoesNotFollowSymlinkFromAnotherNode() throws IOException {
    // A file on the node reading the output, outside the staging directory.
    File secret = new File(testDir, "secret.txt");
    Files.writeString(secret.toPath(), "top secret\n");
    // The job replaces its own output file with an absolute symlink while running.
    String templateName = "symlink_job";
    backend.insert(
        newScriptJobTemplateEntity(
            templateName, "echo \"hello\"\nln -sf \"$1\" output.log\n", "{{target}}"),
        false);

    JobEntity job =
        nodeA.runJob(METALAKE, templateName, ImmutableMap.of("target", secret.getAbsolutePath()));
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.SUCCEEDED);

    Assertions.assertEquals(
        Collections.emptyList(), nodeB.getJob(METALAKE, job.name(), true).stdout());
  }

  @TestTemplate
  public void testGetJobOutputFromNodeNotSharingStagingDir() throws IOException {
    JobEntity job = runEchoJobOnNodeA("c");

    // A node with a staging directory of its own can't reach the output, and reports none
    // instead of failing.
    Config otherConfig = newConfig(new File(testDir, "other-staging"));
    JobManager nodeC = newJobManager(otherConfig, entityStore, newLocalJobExecutor(otherConfig));
    try {
      JobEntity jobWithOutput = nodeC.getJob(METALAKE, job.name(), true);
      Assertions.assertEquals(Collections.emptyList(), jobWithOutput.stdout());
      Assertions.assertEquals(Collections.emptyList(), jobWithOutput.stderr());
    } finally {
      nodeC.close();
    }
  }

  private JobEntity runEchoJobOnNodeA(String name) throws IOException {
    JobEntity job = nodeA.runJob(METALAKE, ECHO_TEMPLATE, ImmutableMap.of("name", name));
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.SUCCEEDED);
    return job;
  }

  @TestTemplate
  public void testStagingDirIsCleanedUpAfterTemplateRenamed() throws IOException {
    JobEntity job = runFinishedJobOnNodeA();
    Assertions.assertTrue(jobStagingDir(job).isDirectory());

    // Before the fix, the cleanup rebuilt the staging path from the new template name, missed the
    // directory and deleted only the job entity, leaking the directory.
    nodeA.alterJobTemplate(METALAKE, TEMPLATE, JobTemplateChange.rename("renamed_sleep_job"));
    Assertions.assertEquals("renamed_sleep_job", getJob(job.name()).jobTemplateName());
    moveJobTimestampsBack(job.name());
    nodeB.cleanUpStagingDirs();

    Assertions.assertFalse(jobExists(job.name()));
    assertNoStagingDirLeft(job);
  }

  @TestTemplate
  public void testStagingDirIsDeletedWithRenamedTemplate() throws IOException {
    JobEntity job = runFinishedJobOnNodeA();

    nodeA.alterJobTemplate(METALAKE, TEMPLATE, JobTemplateChange.rename("renamed_sleep_job"));
    nodeA.alterJobTemplate(
        METALAKE, "renamed_sleep_job", JobTemplateChange.rename("renamed_twice_sleep_job"));
    Assertions.assertTrue(nodeB.deleteJobTemplate(METALAKE, "renamed_twice_sleep_job"));

    Assertions.assertFalse(jobExists(job.name()));
    assertNoStagingDirLeft(job);
  }

  @TestTemplate
  public void testStagingDirIsCleanedUpAfterMetalakeRenamed() throws IOException {
    JobEntity job = runFinishedJobOnNodeA();
    moveJobTimestampsBack(job.name());

    String newMetalake = METALAKE + "_renamed";
    entityStore.update(
        NameIdentifierUtil.ofMetalake(METALAKE),
        BaseMetalake.class,
        Entity.EntityType.METALAKE,
        metalake ->
            BaseMetalake.builder()
                .withId(metalake.id())
                .withName(newMetalake)
                .withComment(metalake.comment())
                .withProperties(metalake.properties())
                .withAuditInfo(metalake.auditInfo())
                .withVersion(metalake.getVersion())
                .build());
    nodeB.cleanUpStagingDirs();

    Assertions.assertThrows(
        NoSuchJobException.class, () -> nodeB.getJob(newMetalake, job.name(), false));
    assertNoStagingDirLeft(job);
  }

  private JobEntity runFinishedJobOnNodeA() throws IOException {
    JobEntity job = nodeA.runJob(METALAKE, TEMPLATE, ImmutableMap.of("seconds", "0"));
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.SUCCEEDED);
    nodeA.pullAndUpdateJobStatus();
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, getJob(job.name()).status());
    return job;
  }

  private File jobStagingDir(JobEntity job) {
    return new File(testDir, "staging/job-runs/" + job.name());
  }

  // Checks the whole staging directory rather than the expected path, so a directory left in any
  // layout is caught.
  private void assertNoStagingDirLeft(JobEntity job) throws IOException {
    try (Stream<Path> paths = Files.walk(new File(testDir, "staging").toPath())) {
      List<Path> left =
          paths
              .filter(path -> path.getFileName().toString().equals(job.name()))
              .collect(Collectors.toList());
      Assertions.assertTrue(left.isEmpty(), "Staging directory left behind: " + left);
    }
  }

  private JobEntity runLongJobOnNodeA() throws IOException {
    JobEntity job = nodeA.runJob(METALAKE, TEMPLATE, ImmutableMap.of("seconds", "600"));
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .until(() -> executorA.getJobStatus(job.jobExecutionId()) == JobHandle.Status.STARTED);
    nodeA.pullAndUpdateJobStatus();
    Assertions.assertEquals(JobHandle.Status.STARTED, getJob(job.name()).status());
    return job;
  }

  private JobEntity getJob(String jobName) {
    // Read through node B, as it works the same from any node sharing the metadata store.
    return nodeB.getJob(METALAKE, jobName, false);
  }

  // Simulates that the keep time has elapsed since the job was last updated, or finished.
  private void moveJobTimestampsBack(String jobName) throws IOException {
    long offsetInMs = JOB_KEEP_TIME_IN_MS + TimeUnit.MINUTES.toMillis(1);
    entityStore.update(
        NameIdentifierUtil.ofJob(METALAKE, jobName),
        JobEntity.class,
        Entity.EntityType.JOB,
        job ->
            JobEntity.builder()
                .withId(job.id())
                .withJobExecutionId(job.jobExecutionId())
                .withJobTemplateName(job.jobTemplateName())
                .withStatus(job.status())
                .withNamespace(job.namespace())
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(job.auditInfo().creator())
                        .withCreateTime(job.auditInfo().createTime().minusMillis(offsetInMs))
                        .withLastModifier(job.auditInfo().lastModifier())
                        .withLastModifiedTime(
                            job.auditInfo().lastModifiedTime() == null
                                ? null
                                : job.auditInfo().lastModifiedTime().minusMillis(offsetInMs))
                        .build())
                .withStartedAt(job.startedAt())
                .withFinishedAt(job.finishedAt() > 0 ? job.finishedAt() - offsetInMs : 0L)
                .withRuntimeJobTemplate(job.runtimeJobTemplate())
                .build());
  }

  private boolean jobExists(String jobName) {
    try {
      getJob(jobName);
      return true;
    } catch (NoSuchJobException e) {
      return false;
    }
  }

  private JobTemplateEntity newSleepJobTemplateEntity() throws IOException {
    // Exec the sleep, so that killing the job process also stops the sleep.
    return newScriptJobTemplateEntity(TEMPLATE, "exec sleep \"$1\"\n", "{{seconds}}");
  }

  private JobTemplateEntity newEchoJobTemplateEntity() throws IOException {
    return newScriptJobTemplateEntity(
        ECHO_TEMPLATE, "echo \"hello $1\"\necho \"oops $1\" >&2\n", "{{name}}");
  }

  private JobTemplateEntity newScriptJobTemplateEntity(
      String name, String scriptBody, String argument) throws IOException {
    // Not named after the template, whose name may contain any character.
    File script = Files.createTempFile(testDir.toPath(), "job", ".sh").toFile();
    Files.writeString(script.toPath(), "#!/bin/bash\n" + scriptBody);
    Assertions.assertTrue(script.setExecutable(true));

    return JobTemplateEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(METALAKE))
        .withTemplateContent(
            JobTemplateEntity.TemplateContent.builder()
                .withJobType(JobTemplate.JobType.SHELL)
                .withExecutable(script.getAbsolutePath())
                .withArguments(Lists.newArrayList(argument))
                .withEnvironments(Collections.emptyMap())
                .withCustomFields(Collections.emptyMap())
                .withScripts(Collections.emptyList())
                .build())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private static Config newConfig(File stagingDir) {
    Config config = new Config(false) {};
    config.set(Configs.JOB_STAGING_DIR, stagingDir.getAbsolutePath());
    config.set(Configs.JOB_STAGING_DIR_KEEP_TIME_IN_MS, JOB_KEEP_TIME_IN_MS);
    return config;
  }

  // Configured the way JobExecutorFactory configures it for a server with this configuration.
  private static LocalJobExecutor newLocalJobExecutor(Config config) {
    LocalJobExecutor executor = new LocalJobExecutor();
    executor.initialize(
        ImmutableMap.of(LocalJobExecutorConfigs.STAGING_DIR, config.get(Configs.JOB_STAGING_DIR)));
    return executor;
  }

  private static JobManager newJobManager(
      Config config, EntityStore entityStore, JobExecutor jobExecutor) {
    JobManager jobManager =
        new JobManager(config, entityStore, RandomIdGenerator.INSTANCE, jobExecutor);
    // Stop the background schedulers, the test pulls the job status manually.
    jobManager.statusPullExecutor.shutdownNow();
    jobManager.cleanUpExecutor.shutdownNow();
    return jobManager;
  }
}
