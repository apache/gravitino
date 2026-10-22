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
package org.apache.gravitino.job.k8s.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.k8s.K8sJobResourceUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestSparkApplicationStatusUtils {

  private static final Instant CREATED_AT = Instant.parse("2026-09-30T00:00:00Z");

  private static final Instant NOW = CREATED_AT.plusSeconds(60);

  private static final long NO_STATUS_TIMEOUT_MS = 600_000L;

  @ParameterizedTest
  @CsvSource({
    "Submitted, QUEUED",
    "ScheduledToRestart, QUEUED",
    "DriverRequested, QUEUED",
    "DriverStarted, STARTED",
    "DriverReady, STARTED",
    "InitializedBelowThresholdExecutors, STARTED",
    "RunningHealthy, STARTED",
    "RunningWithPartialCapacity, STARTED",
    "RunningWithBelowThresholdExecutors, STARTED",
    "Succeeded, SUCCEEDED",
    "Failed, FAILED",
    "SchedulingFailure, FAILED",
    "DriverEvicted, FAILED",
    "DriverStartTimedOut, FAILED",
    "ExecutorsStartTimedOut, FAILED",
    "DriverReadyTimedOut, FAILED",
    "ResourceReleased, FAILED",
    "TerminatedWithoutReleaseResources, FAILED"
  })
  public void testCurrentState(String state, JobHandle.Status expected) {
    GenericKubernetesResource app = app("Submitted", state);
    Assertions.assertEquals(expected, executionInfoOf(app).status());
  }

  @Test
  public void testUnknownState() {
    Assertions.assertEquals(
        JobHandle.Status.QUEUED, executionInfoOf(app("Submitted", "SomeNewState")).status());
    Assertions.assertEquals(
        JobHandle.Status.STARTED,
        executionInfoOf(app("Submitted", "DriverReady", "SomeNewState")).status());
  }

  @Test
  public void testNoStatus() {
    GenericKubernetesResource app = app();
    Assertions.assertEquals(JobHandle.Status.QUEUED, executionInfoOf(app).status());
    Assertions.assertEquals(
        JobHandle.Status.FAILED,
        SparkApplicationStatusUtils.executionInfoOf(
                app, null, CREATED_AT.plusMillis(NO_STATUS_TIMEOUT_MS + 1), NO_STATUS_TIMEOUT_MS)
            .status());
  }

  @Test
  public void testUnreadableStatus() {
    // A status without any state this version can read, for example of a newer operator, keeps
    // the job unfinished after the no-status timeout.
    GenericKubernetesResource app = app();
    app.setAdditionalProperty("status", ImmutableMap.of("phase", "Running"));
    Assertions.assertTrue(SparkApplicationStatusUtils.hasStatus(app));
    Assertions.assertFalse(SparkApplicationStatusUtils.hasState(app));
    Assertions.assertEquals(
        JobHandle.Status.QUEUED,
        SparkApplicationStatusUtils.executionInfoOf(
                app, null, CREATED_AT.plusMillis(NO_STATUS_TIMEOUT_MS + 1), NO_STATUS_TIMEOUT_MS)
            .status());

    // An empty status is no status.
    app.setAdditionalProperty("status", ImmutableMap.of());
    Assertions.assertFalse(SparkApplicationStatusUtils.hasStatus(app));
    Assertions.assertEquals(
        JobHandle.Status.FAILED,
        SparkApplicationStatusUtils.executionInfoOf(
                app, null, CREATED_AT.plusMillis(NO_STATUS_TIMEOUT_MS + 1), NO_STATUS_TIMEOUT_MS)
            .status());
  }

  @Test
  public void testTimes() {
    JobExecutionInfo queued = executionInfoOf(app("Submitted", "DriverRequested"));
    Assertions.assertNull(queued.startedAt());
    Assertions.assertNull(queued.finishedAt());

    JobExecutionInfo running =
        executionInfoOf(app("Submitted", "DriverRequested", "DriverReady", "RunningHealthy"));
    Assertions.assertEquals(JobHandle.Status.STARTED, running.status());
    Assertions.assertEquals(timeOf(2), running.startedAt());
    Assertions.assertNull(running.finishedAt());

    JobExecutionInfo succeeded =
        executionInfoOf(
            app(
                "Submitted",
                "DriverRequested",
                "DriverReady",
                "RunningHealthy",
                "Succeeded",
                "TerminatedWithoutReleaseResources",
                "ResourceReleased"));
    Assertions.assertEquals(
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withStartedAt(timeOf(2))
            .withFinishedAt(timeOf(4))
            .build(),
        succeeded);

    JobExecutionInfo neverStarted =
        executionInfoOf(app("Submitted", "DriverRequested", "DriverStartTimedOut"));
    Assertions.assertEquals(JobHandle.Status.FAILED, neverStarted.status());
    Assertions.assertNull(neverStarted.startedAt());
    Assertions.assertEquals(timeOf(2), neverStarted.finishedAt());
  }

  @Test
  public void testDriverContainerTimes() {
    Instant driverStartedAt = Instant.parse("2026-09-30T00:00:01Z");
    Instant driverFinishedAt = Instant.parse("2026-09-30T00:00:02Z");

    // The operator observed the driver running when it was ready.
    GenericKubernetesResource running = app("Submitted", "DriverReady", "RunningHealthy");
    Map<String, Map<String, Object>> history = history(running);
    history.put(
        "1",
        withDriverStatus(
            history.get("1"),
            ImmutableMap.of("running", ImmutableMap.of("startedAt", driverStartedAt.toString()))));
    JobExecutionInfo info = executionInfoOf(running);
    Assertions.assertEquals(JobHandle.Status.STARTED, info.status());
    Assertions.assertEquals(driverStartedAt, info.startedAt());

    // The operator observed the driver terminated a while after it finished.
    GenericKubernetesResource failed =
        app("Submitted", "DriverReady", "Failed", "TerminatedWithoutReleaseResources");
    history = history(failed);
    history.put(
        "2",
        withDriverStatus(
            history.get("2"),
            ImmutableMap.of(
                "terminated",
                ImmutableMap.of(
                    "startedAt", driverStartedAt.toString(),
                    "finishedAt", driverFinishedAt.toString(),
                    "exitCode", 1))));
    Assertions.assertEquals(
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.FAILED)
            .withStartedAt(driverStartedAt)
            .withFinishedAt(driverFinishedAt)
            .build(),
        executionInfoOf(failed));
  }

  @Test
  public void testRenamedDriverContainer() {
    Instant driverFinishedAt = Instant.parse("2026-09-30T00:00:02Z");
    GenericKubernetesResource succeeded = app("Submitted", "DriverReady", "Succeeded");
    Map<String, Map<String, Object>> history = history(succeeded);
    Map<String, Object> terminated =
        ImmutableMap.of(
            "terminated",
            ImmutableMap.of(
                "startedAt", "2026-09-30T00:00:01Z", "finishedAt", driverFinishedAt.toString()));
    history.put(
        "2",
        ImmutableMap.<String, Object>builder()
            .putAll(history.get("2"))
            .put(
                "lastObservedDriverStatus",
                ImmutableMap.of(
                    "containerStatuses",
                    ImmutableList.of(ImmutableMap.of("name", "main", "state", terminated))))
            .build());
    Assertions.assertEquals(driverFinishedAt, executionInfoOf(succeeded).finishedAt());

    // With sidecars, the driver container can't be told apart.
    history.put(
        "2",
        ImmutableMap.<String, Object>builder()
            .putAll(history(app("Submitted", "DriverReady", "Succeeded")).get("2"))
            .put(
                "lastObservedDriverStatus",
                ImmutableMap.of(
                    "containerStatuses",
                    ImmutableList.of(
                        ImmutableMap.of("name", "main", "state", terminated),
                        ImmutableMap.of("name", "sidecar", "state", terminated))))
            .build());
    Assertions.assertEquals(timeOf(2), executionInfoOf(succeeded).finishedAt());
  }

  @Test
  public void testDriverPodTimes() {
    GenericKubernetesResource succeeded =
        app("Submitted", "DriverReady", "Succeeded", "TerminatedWithoutReleaseResources");
    Pod driver =
        new PodBuilder()
            .withNewStatus()
            .addNewContainerStatus()
            .withName("spark-kubernetes-driver")
            .withNewState()
            .withNewTerminated()
            .withStartedAt("2026-09-30T00:00:00Z")
            .withFinishedAt("2026-09-30T00:00:01Z")
            .endTerminated()
            .endState()
            .endContainerStatus()
            .endStatus()
            .build();

    Assertions.assertEquals(
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withStartedAt(Instant.parse("2026-09-30T00:00:00Z"))
            .withFinishedAt(Instant.parse("2026-09-30T00:00:01Z"))
            .build(),
        SparkApplicationStatusUtils.executionInfoOf(succeeded, driver, NOW, NO_STATUS_TIMEOUT_MS));

    // A driver pod without status falls back to the operator's times.
    Assertions.assertEquals(
        timeOf(2),
        SparkApplicationStatusUtils.executionInfoOf(
                succeeded, new PodBuilder().build(), NOW, NO_STATUS_TIMEOUT_MS)
            .finishedAt());
  }

  @Test
  public void testStoppedStatesWalkBackToOutcome() {
    Assertions.assertEquals(
        JobHandle.Status.FAILED,
        executionInfoOf(app("Submitted", "DriverReady", "Failed", "ResourceReleased")).status());
    Assertions.assertEquals(
        JobHandle.Status.SUCCEEDED,
        executionInfoOf(app("Submitted", "DriverReady", "Succeeded", "ResourceReleased")).status());
    Assertions.assertEquals(
        JobHandle.Status.FAILED,
        executionInfoOf(app("Submitted", "DriverReady", "ResourceReleased")).status());
  }

  @Test
  public void testHistoryOrderedById() {
    GenericKubernetesResource app = app("Submitted", "DriverReady", "Succeeded");
    // Ids sort numerically, not as strings.
    Map<String, Object> history = new LinkedHashMap<>();
    for (int i = 0; i < 12; i++) {
      history.put(String.valueOf(11 - i), state(i == 0 ? "Succeeded" : "RunningHealthy", 11 - i));
    }
    history.put("0", state("Submitted", 0));
    status(app).put("stateTransitionHistory", history);
    status(app).put("currentState", state("Succeeded", 11));

    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, info.status());
    Assertions.assertEquals(timeOf(1), info.startedAt());
    Assertions.assertEquals(timeOf(11), info.finishedAt());
  }

  @Test
  public void testCurrentStateMissingFromHistory() {
    GenericKubernetesResource app = app("Submitted", "DriverReady");
    status(app).put("currentState", state("Succeeded", 5));
    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, info.status());
    Assertions.assertEquals(timeOf(5), info.finishedAt());
  }

  @Test
  public void testCancelled() {
    Instant deletedAt = NOW.minusSeconds(1);

    // Cancel requested, but the deletion hasn't taken effect yet.
    GenericKubernetesResource running = app("Submitted", "DriverReady");
    cancel(running);
    Assertions.assertTrue(K8sJobResourceUtils.isCancelRequested(running));
    Assertions.assertEquals(JobHandle.Status.STARTED, executionInfoOf(running).status());

    running.getMetadata().setDeletionTimestamp(deletedAt.toString());
    JobExecutionInfo cancelled = executionInfoOf(running);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelled.status());
    Assertions.assertEquals(timeOf(1), cancelled.startedAt());
    Assertions.assertEquals(deletedAt, cancelled.finishedAt());

    // The application stopped by itself after it was marked, but before the deletion.
    GenericKubernetesResource stopped = app("Submitted", "DriverReady", "Failed");
    cancel(stopped);
    stopped.getMetadata().setDeletionTimestamp(timeOf(3).toString());
    JobExecutionInfo info = executionInfoOf(stopped);
    Assertions.assertEquals(JobHandle.Status.FAILED, info.status());
    Assertions.assertEquals(timeOf(2), info.finishedAt());

    // An outcome recorded in the second of the deletion counts as after it.
    GenericKubernetesResource sameSecond = app("Submitted", "DriverReady", "Failed");
    cancel(sameSecond);
    sameSecond.getMetadata().setDeletionTimestamp(CREATED_AT.plusSeconds(2).toString());
    Assertions.assertEquals(JobHandle.Status.CANCELLED, executionInfoOf(sameSecond).status());
  }

  @Test
  public void testCancelMarkWithoutDeletion() {
    // A cancellation that failed after marking the job, for example because its response was
    // lost, leaves the mark. The job keeps running and keeps the outcome it reaches.
    GenericKubernetesResource succeeded = app("Submitted", "DriverReady", "Succeeded");
    cancel(succeeded);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, executionInfoOf(succeeded).status());

    GenericKubernetesResource failed = app("Submitted", "DriverReady", "Failed");
    cancel(failed);
    Assertions.assertEquals(JobHandle.Status.FAILED, executionInfoOf(failed).status());

    GenericKubernetesResource running = app("Submitted", "DriverReady", "RunningHealthy");
    cancel(running);
    Assertions.assertEquals(JobHandle.Status.STARTED, executionInfoOf(running).status());

    // Deleted by its TTL after it succeeded, it still succeeded.
    succeeded.getMetadata().setDeletionTimestamp(NOW.toString());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, executionInfoOf(succeeded).status());
  }

  @Test
  public void testDeletedBySomeoneElse() {
    Instant deletedAt = NOW.minusSeconds(1);

    GenericKubernetesResource succeeded = app("Submitted", "DriverReady", "Succeeded");
    succeeded.getMetadata().setDeletionTimestamp(deletedAt.toString());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, executionInfoOf(succeeded).status());

    GenericKubernetesResource running = app("Submitted", "DriverReady");
    running.getMetadata().setDeletionTimestamp(deletedAt.toString());
    JobExecutionInfo info = executionInfoOf(running);
    Assertions.assertEquals(JobHandle.Status.FAILED, info.status());
    Assertions.assertEquals(deletedAt, info.finishedAt());
  }

  @Test
  public void testIsStopped() {
    Assertions.assertFalse(SparkApplicationStatusUtils.isStopped(app()));
    Assertions.assertFalse(SparkApplicationStatusUtils.isStopped(app("Submitted", "DriverReady")));
    Assertions.assertTrue(SparkApplicationStatusUtils.isStopped(app("Submitted", "Succeeded")));
    Assertions.assertTrue(
        SparkApplicationStatusUtils.isStopped(app("Submitted", "DriverStartTimedOut")));
    Assertions.assertTrue(
        SparkApplicationStatusUtils.isStopped(app("Submitted", "ResourceReleased")));
  }

  @Test
  public void testDriverStartedIsStarted() {
    // The driver container runs from DriverStarted on, even if the pod never gets ready.
    GenericKubernetesResource app = app("Submitted", "DriverRequested", "DriverStarted");
    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.STARTED, info.status());
    Assertions.assertEquals(timeOf(2), info.startedAt());

    Pod running = driverPod("spark-kubernetes-driver", "2026-09-30T00:00:01Z", null);
    JobExecutionInfo withPod =
        SparkApplicationStatusUtils.executionInfoOf(app, running, NOW, NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(JobHandle.Status.STARTED, withPod.status());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:01Z"), withPod.startedAt());

    // Cancelled before DriverReady, the job still has its started time.
    cancel(app);
    app.getMetadata().setDeletionTimestamp(CREATED_AT.plusSeconds(10).toString());
    JobExecutionInfo cancelled = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelled.status());
    Assertions.assertEquals(timeOf(2), cancelled.startedAt());
    Assertions.assertEquals(CREATED_AT.plusSeconds(10), cancelled.finishedAt());

    // An unknown state after DriverStarted is a started one.
    Assertions.assertEquals(
        JobHandle.Status.STARTED,
        executionInfoOf(app("Submitted", "DriverStarted", "SomeNewState")).status());
  }

  @Test
  public void testDriverTerminationTimeWithoutOutcome() {
    // The driver terminated after the deletion was requested, and the operator recorded no
    // outcome. The time the driver actually terminated is the finished time.
    Pod terminated =
        driverPod("spark-kubernetes-driver", "2026-09-30T00:00:02Z", "2026-09-30T00:00:25Z");

    GenericKubernetesResource cancelled = app("Submitted", "DriverReady", "RunningHealthy");
    cancel(cancelled);
    cancelled.getMetadata().setDeletionTimestamp(CREATED_AT.plusSeconds(20).toString());
    JobExecutionInfo info =
        SparkApplicationStatusUtils.executionInfoOf(
            cancelled, terminated, NOW, NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, info.status());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:25Z"), info.finishedAt());

    // Deleted by someone else: the job failed, a terminated driver doesn't make it succeed.
    GenericKubernetesResource deleted = app("Submitted", "DriverReady", "RunningHealthy");
    deleted.getMetadata().setDeletionTimestamp(CREATED_AT.plusSeconds(20).toString());
    info =
        SparkApplicationStatusUtils.executionInfoOf(deleted, terminated, NOW, NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(JobHandle.Status.FAILED, info.status());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:25Z"), info.finishedAt());

    // Without the driver pod, the deletion time is the best known one.
    Assertions.assertEquals(CREATED_AT.plusSeconds(20), executionInfoOf(deleted).finishedAt());

    // A running job isn't finished because a driver pod terminated.
    info =
        SparkApplicationStatusUtils.executionInfoOf(
            app("Submitted", "DriverReady", "RunningHealthy"),
            terminated,
            NOW,
            NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(JobHandle.Status.STARTED, info.status());
    Assertions.assertNull(info.finishedAt());
  }

  @Test
  public void testUnknownStateAfterOutcome() {
    // A newer operator may add a state after the outcome, the job must not stay unfinished.
    GenericKubernetesResource app = app("Submitted", "DriverReady", "Succeeded", "SomeNewState");
    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, info.status());
    Assertions.assertEquals(timeOf(2), info.finishedAt());
    Assertions.assertTrue(SparkApplicationStatusUtils.isStopped(app));

    // A known state after an outcome means the application runs again.
    GenericKubernetesResource restarted =
        app("Submitted", "DriverReady", "Failed", "ScheduledToRestart", "RunningHealthy");
    Assertions.assertEquals(JobHandle.Status.STARTED, executionInfoOf(restarted).status());
    Assertions.assertNull(executionInfoOf(restarted).finishedAt());
    Assertions.assertFalse(SparkApplicationStatusUtils.isStopped(restarted));
  }

  @Test
  public void testStoppedWithoutOutcome() {
    GenericKubernetesResource app = app("Submitted", "DriverReady", "ResourceReleased");
    JobExecutionInfo failed = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.FAILED, failed.status());
    Assertions.assertEquals(timeOf(2), failed.finishedAt());

    // Stopped before it was cancelled, it isn't cancelled.
    cancel(app);
    app.getMetadata().setDeletionTimestamp(NOW.toString());
    Assertions.assertEquals(JobHandle.Status.FAILED, executionInfoOf(app).status());
    Assertions.assertEquals(timeOf(2), executionInfoOf(app).finishedAt());

    GenericKubernetesResource stopped =
        app("Submitted", "DriverReady", "Succeeded", "TerminatedWithoutReleaseResources");
    cancel(stopped);
    stopped.getMetadata().setDeletionTimestamp(NOW.toString());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, executionInfoOf(stopped).status());
    Assertions.assertEquals(timeOf(2), executionInfoOf(stopped).finishedAt());
  }

  @Test
  public void testStoppedStateWithoutTime() {
    GenericKubernetesResource app = app("Submitted", "DriverReady", "ResourceReleased");
    history(app)
        .put(
            "2",
            ImmutableMap.of(
                "currentStateSummary", "ResourceReleased", "lastTransitionTime", "soon"));
    status(app).put("currentState", history(app).get("2"));
    app.getMetadata().setDeletionTimestamp(NOW.toString());

    // Deleted after it stopped at an unknown time, it finished when it was deleted at the latest.
    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.FAILED, info.status());
    Assertions.assertEquals(NOW, info.finishedAt());

    // Nor is it cancelled.
    cancel(app);
    Assertions.assertEquals(JobHandle.Status.FAILED, executionInfoOf(app).status());
  }

  @Test
  public void testDeletionNotBeforeStart() {
    // The deletion time is in whole seconds, the started time of the same second isn't.
    GenericKubernetesResource app = app("Submitted", "DriverReady");
    cancel(app);
    app.getMetadata().setDeletionTimestamp(CREATED_AT.plusSeconds(1).toString());

    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, info.status());
    Assertions.assertEquals(timeOf(1), info.startedAt());
    Assertions.assertEquals(timeOf(1), info.finishedAt());
  }

  @Test
  public void testContainerStartBeforeCreationIsIgnored() {
    // A container that failed to start reports a start time of the epoch.
    Map<String, Object> neverStarted =
        ImmutableMap.of(
            "terminated",
            ImmutableMap.of(
                "startedAt", "1970-01-01T00:00:00Z", "finishedAt", "2026-09-30T00:00:02Z"));

    GenericKubernetesResource app = app("Submitted", "DriverRequested", "Failed");
    Map<String, Map<String, Object>> history = history(app);
    history.put("2", withDriverStatus(history.get("2"), neverStarted));
    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.FAILED, info.status());
    Assertions.assertNull(info.startedAt());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:02Z"), info.finishedAt());

    // The operator saw it started, so its time is used instead.
    GenericKubernetesResource started = app("Submitted", "DriverReady", "Failed");
    history = history(started);
    history.put("2", withDriverStatus(history.get("2"), neverStarted));
    Assertions.assertEquals(timeOf(1), executionInfoOf(started).startedAt());

    Pod driver = driverPod("spark-kubernetes-driver", "1970-01-01T00:00:00Z", null);
    Assertions.assertEquals(
        timeOf(1),
        SparkApplicationStatusUtils.executionInfoOf(
                app("Submitted", "DriverReady", "Succeeded"), driver, NOW, NO_STATUS_TIMEOUT_MS)
            .startedAt());
  }

  @Test
  public void testDriverPodStates() {
    GenericKubernetesResource app = app("Submitted", "DriverReady", "Failed");
    Map<String, Map<String, Object>> history = history(app);
    history.put(
        "2",
        withDriverStatus(
            history.get("2"),
            ImmutableMap.of(
                "terminated",
                ImmutableMap.of(
                    "startedAt", "2026-09-30T00:00:10Z", "finishedAt", "2026-09-30T00:00:20Z"))));

    // The driver pod wins over the driver status that the operator observed.
    JobExecutionInfo terminated =
        SparkApplicationStatusUtils.executionInfoOf(
            app,
            driverPod("spark-kubernetes-driver", "2026-09-30T00:00:11Z", "2026-09-30T00:00:21Z"),
            NOW,
            NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:11Z"), terminated.startedAt());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:21Z"), terminated.finishedAt());

    // A running driver has a started time only, the finished time is the observed one.
    JobExecutionInfo running =
        SparkApplicationStatusUtils.executionInfoOf(
            app,
            driverPod("spark-kubernetes-driver", "2026-09-30T00:00:12Z", null),
            NOW,
            NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:12Z"), running.startedAt());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:20Z"), running.finishedAt());

    // The only container is the driver, whatever its name.
    JobExecutionInfo renamed =
        SparkApplicationStatusUtils.executionInfoOf(
            app,
            driverPod("main", "2026-09-30T00:00:13Z", "2026-09-30T00:00:23Z"),
            NOW,
            NO_STATUS_TIMEOUT_MS);
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:23Z"), renamed.finishedAt());
  }

  @Test
  public void testDriverContainerNamedByJob() {
    // With a sidecar, the driver container is the one the job names for its pod template.
    GenericKubernetesResource app = app("Submitted", "DriverReady", "Failed");
    app.setAdditionalProperty(
        "spec",
        ImmutableMap.of(
            "sparkConf",
            ImmutableMap.of(SparkApplicationUtils.SPARK_DRIVER_CONTAINER_NAME, "main")));
    Map<String, Object> driver =
        ImmutableMap.of(
            "terminated",
            ImmutableMap.of(
                "startedAt", "2026-09-30T00:00:10Z", "finishedAt", "2026-09-30T00:00:20Z"));
    Map<String, Object> sidecar =
        ImmutableMap.of(
            "terminated",
            ImmutableMap.of(
                "startedAt", "2026-09-30T00:00:05Z", "finishedAt", "2026-09-30T00:00:30Z"));
    Map<String, Map<String, Object>> history = history(app);
    history.put(
        "2",
        ImmutableMap.<String, Object>builder()
            .putAll(history.get("2"))
            .put(
                "lastObservedDriverStatus",
                ImmutableMap.of(
                    "containerStatuses",
                    ImmutableList.of(
                        ImmutableMap.of("name", "sidecar", "state", sidecar),
                        ImmutableMap.of("name", "main", "state", driver))))
            .build());

    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:10Z"), info.startedAt());
    Assertions.assertEquals(Instant.parse("2026-09-30T00:00:20Z"), info.finishedAt());

    Pod pod =
        new PodBuilder(driverPod("sidecar", "2026-09-30T00:00:05Z", "2026-09-30T00:00:30Z"))
            .editStatus()
            .addNewContainerStatus()
            .withName("main")
            .withNewState()
            .withNewTerminated()
            .withStartedAt("2026-09-30T00:00:11Z")
            .withFinishedAt("2026-09-30T00:00:21Z")
            .endTerminated()
            .endState()
            .endContainerStatus()
            .endStatus()
            .build();
    Assertions.assertEquals(
        Instant.parse("2026-09-30T00:00:21Z"),
        SparkApplicationStatusUtils.executionInfoOf(app, pod, NOW, NO_STATUS_TIMEOUT_MS)
            .finishedAt());
  }

  @Test
  public void testHasState() {
    Assertions.assertFalse(SparkApplicationStatusUtils.hasState(app()));
    Assertions.assertTrue(SparkApplicationStatusUtils.hasState(app("Submitted")));

    GenericKubernetesResource unreadable = app();
    unreadable.setAdditionalProperty("status", "not an object");
    Assertions.assertFalse(SparkApplicationStatusUtils.hasState(unreadable));
    unreadable.setAdditionalProperty(
        "status", ImmutableMap.of("currentState", ImmutableMap.of("message", "no summary")));
    Assertions.assertFalse(SparkApplicationStatusUtils.hasState(unreadable));
  }

  @Test
  public void testCurrentStateWithoutHistory() {
    GenericKubernetesResource app = app();
    app.setAdditionalProperty(
        "status", ImmutableMap.of("currentState", state("RunningHealthy", 3)));

    Assertions.assertTrue(SparkApplicationStatusUtils.hasState(app));
    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.STARTED, info.status());
    Assertions.assertEquals(timeOf(3), info.startedAt());
  }

  @Test
  public void testMalformedStatus() {
    GenericKubernetesResource app = app("Submitted", "DriverReady", "Succeeded");
    Map<String, Map<String, Object>> history = history(app);
    // Entries that can't be read are skipped, and a time that can't be parsed is unknown.
    history.put("not-a-number", state("Failed", 9));
    history.put("7", ImmutableMap.of("message", "no summary"));
    history.put(
        "2", ImmutableMap.of("currentStateSummary", "Succeeded", "lastTransitionTime", "soon"));

    JobExecutionInfo info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, info.status());
    Assertions.assertEquals(timeOf(1), info.startedAt());
    Assertions.assertNull(info.finishedAt());

    // Marked and deleted, it had still stopped before, as its outcome time is unknown. It finished
    // when it was deleted at the latest.
    cancel(app);
    app.getMetadata().setDeletionTimestamp(NOW.toString());
    info = executionInfoOf(app);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, info.status());
    Assertions.assertEquals(NOW, info.finishedAt());

    // Without a creation time, an application without status never times out.
    GenericKubernetesResource noCreationTime = app();
    noCreationTime.getMetadata().setCreationTimestamp(null);
    Assertions.assertEquals(
        JobHandle.Status.QUEUED,
        SparkApplicationStatusUtils.executionInfoOf(
                noCreationTime, null, NOW.plusSeconds(86_400), NO_STATUS_TIMEOUT_MS)
            .status());
  }

  @Test
  public void testStatusReportedByOperator() {
    // The status of a failed application as operator 0.9.0 reports it, read the way the client
    // does: the history is keyed by numbers in strings, the times have nanoseconds, and only the
    // Failed state carries the driver status.
    String json =
        "{\"apiVersion\":\"spark.apache.org/v1\",\"kind\":\"SparkApplication\","
            + "\"metadata\":{\"name\":\"gravitino-job-1\",\"namespace\":\"default\","
            + "\"creationTimestamp\":\"2026-09-24T09:22:32Z\"},"
            + "\"status\":{\"currentState\":{\"currentStateSummary\":\"ResourceReleased\","
            + "\"lastTransitionTime\":\"2026-09-27T01:02:22.906216807Z\"},"
            + "\"stateTransitionHistory\":{"
            + "\"10\":{\"currentStateSummary\":\"ResourceReleased\","
            + "\"lastTransitionTime\":\"2026-09-27T01:02:22.906216807Z\"},"
            + "\"0\":{\"currentStateSummary\":\"Submitted\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:32.207220731Z\"},"
            + "\"1\":{\"currentStateSummary\":\"DriverRequested\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:32.436928096Z\"},"
            + "\"2\":{\"currentStateSummary\":\"DriverStarted\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:33.099430159Z\"},"
            + "\"3\":{\"currentStateSummary\":\"DriverReady\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:33.099616659Z\"},"
            + "\"4\":{\"currentStateSummary\":\"RunningHealthy\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:33.111225423Z\"},"
            + "\"5\":{\"currentStateSummary\":\"Failed\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:57.002342477Z\","
            + "\"lastObservedDriverStatus\":{\"phase\":\"Failed\",\"containerStatuses\":["
            + "{\"name\":\"spark-kubernetes-driver\",\"state\":{\"terminated\":{\"exitCode\":101,"
            + "\"startedAt\":\"2026-09-24T09:22:32Z\",\"finishedAt\":\"2026-09-24T09:22:33Z\"}}}]}},"
            + "\"6\":{\"currentStateSummary\":\"TerminatedWithoutReleaseResources\","
            + "\"lastTransitionTime\":\"2026-09-24T09:22:57.047040866Z\"}}}}";
    GenericKubernetesResource app = Serialization.unmarshal(json, GenericKubernetesResource.class);

    Assertions.assertTrue(SparkApplicationStatusUtils.hasState(app));
    Assertions.assertTrue(SparkApplicationStatusUtils.isStopped(app));
    Assertions.assertEquals(
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.FAILED)
            .withStartedAt(Instant.parse("2026-09-24T09:22:32Z"))
            .withFinishedAt(Instant.parse("2026-09-24T09:22:33Z"))
            .build(),
        executionInfoOf(app));
  }

  private static JobExecutionInfo executionInfoOf(GenericKubernetesResource app) {
    return SparkApplicationStatusUtils.executionInfoOf(app, null, NOW, NO_STATUS_TIMEOUT_MS);
  }

  /** Creates a SparkApplication that went through the given states, one second apart. */
  private static GenericKubernetesResource app(String... states) {
    GenericKubernetesResource app =
        new GenericKubernetesResourceBuilder()
            .withNewMetadata()
            .withNamespace("default")
            .withName("gravitino-job-1")
            .withCreationTimestamp(CREATED_AT.toString())
            .endMetadata()
            .build();
    if (states.length == 0) {
      return app;
    }

    Map<String, Object> history = new LinkedHashMap<>();
    for (int i = 0; i < states.length; i++) {
      history.put(String.valueOf(i), state(states[i], i));
    }
    Map<String, Object> status = new LinkedHashMap<>();
    status.put("currentState", state(states[states.length - 1], states.length - 1));
    status.put("stateTransitionHistory", history);
    app.setAdditionalProperty("status", status);
    return app;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Object>> history(GenericKubernetesResource app) {
    return (Map<String, Map<String, Object>>) status(app).get("stateTransitionHistory");
  }

  private static Map<String, Object> withDriverStatus(
      Map<String, Object> state, Map<String, Object> driverState) {
    return ImmutableMap.<String, Object>builder()
        .putAll(state)
        .put(
            "lastObservedDriverStatus",
            ImmutableMap.of(
                "containerStatuses",
                ImmutableList.of(
                    ImmutableMap.of("name", "spark-kubernetes-driver", "state", driverState))))
        .build();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> status(GenericKubernetesResource app) {
    return (Map<String, Object>) app.getAdditionalProperties().get("status");
  }

  private static Map<String, Object> state(String summary, int index) {
    return ImmutableMap.of(
        "currentStateSummary", summary, "lastTransitionTime", timeOf(index).toString());
  }

  private static Instant timeOf(int index) {
    return CREATED_AT.plusSeconds(index).plusNanos(123_456_789L);
  }

  private static Pod driverPod(String container, String startedAt, String finishedAt) {
    PodBuilder pod = new PodBuilder().withNewStatus().endStatus();
    if (finishedAt == null) {
      return pod.editStatus()
          .addNewContainerStatus()
          .withName(container)
          .withNewState()
          .withNewRunning()
          .withStartedAt(startedAt)
          .endRunning()
          .endState()
          .endContainerStatus()
          .endStatus()
          .build();
    }
    return pod.editStatus()
        .addNewContainerStatus()
        .withName(container)
        .withNewState()
        .withNewTerminated()
        .withStartedAt(startedAt)
        .withFinishedAt(finishedAt)
        .endTerminated()
        .endState()
        .endContainerStatus()
        .endStatus()
        .build();
  }

  private static void cancel(GenericKubernetesResource app) {
    app.getMetadata()
        .setAnnotations(ImmutableMap.of(K8sJobResourceUtils.ANNOTATION_CANCEL_REQUESTED, "true"));
  }
}
