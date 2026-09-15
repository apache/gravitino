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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
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
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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

  // Active jobs not updated for this long are expired, and finished jobs are cleaned up after it.
  // The tests move the job timestamps back instead of waiting for it to elapse.
  private static final long JOB_KEEP_TIME_IN_MS = TimeUnit.HOURS.toMillis(1);

  private File testDir;

  private LocalJobExecutor executorA;

  private LocalJobExecutor executorB;

  private JobManager nodeA;

  private JobManager nodeB;

  private EntityStore entityStore;

  private Object originalConfig;

  private Object originalLockManager;

  @BeforeAll
  public void saveGravitinoEnv() throws IllegalAccessException {
    // The backend extension and this test replace the global config and lock manager, restore them
    // afterwards so that other tests running in the same JVM are not affected.
    originalConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    originalLockManager = FieldUtils.readField(GravitinoEnv.getInstance(), "lockManager", true);
  }

  @AfterAll
  public void restoreGravitinoEnv() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", originalConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", originalLockManager, true);
  }

  @BeforeEach
  public void setUpNodes() throws Exception {
    testDir = Files.createTempDirectory("gravitino-test-job-multi-node").toFile();

    Config config = new Config(false) {};
    config.set(Configs.JOB_STAGING_DIR, new File(testDir, "staging").getAbsolutePath());
    config.set(Configs.JOB_STAGING_DIR_KEEP_TIME_IN_MS, JOB_KEEP_TIME_IN_MS);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    // Both nodes share the same metadata store, backed by the relational backend under test.
    RelationalEntityStore relationalEntityStore = new RelationalEntityStore();
    FieldUtils.writeField(relationalEntityStore, "backend", backend, true);
    FieldUtils.writeField(relationalEntityStore, "cache", new NoOpsCache(config), true);
    entityStore = relationalEntityStore;

    createAndInsertMakeLake(METALAKE);
    backend.insert(newSleepJobTemplateEntity(), false);

    executorA = new LocalJobExecutor();
    executorA.initialize(Collections.emptyMap());
    executorB = new LocalJobExecutor();
    executorB.initialize(Collections.emptyMap());
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
    Assertions.assertEquals(JobHandle.Status.CANCELLING, getJob(job.name()).status());

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
    return nodeB.getJob(METALAKE, jobName);
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
                .withFinishedAt(job.finishedAt() > 0 ? job.finishedAt() - offsetInMs : 0L)
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
    File script = new File(testDir, "sleep-job.sh");
    // Exec the sleep, so that killing the job process also stops the sleep.
    Files.writeString(script.toPath(), "#!/bin/bash\nexec sleep \"$1\"\n");
    Assertions.assertTrue(script.setExecutable(true));

    return JobTemplateEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(TEMPLATE)
        .withNamespace(NamespaceUtil.ofJobTemplate(METALAKE))
        .withTemplateContent(
            JobTemplateEntity.TemplateContent.builder()
                .withJobType(JobTemplate.JobType.SHELL)
                .withExecutable(script.getAbsolutePath())
                .withArguments(Lists.newArrayList("{{seconds}}"))
                .withEnvironments(Collections.emptyMap())
                .withCustomFields(Collections.emptyMap())
                .withScripts(Collections.emptyList())
                .build())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
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
