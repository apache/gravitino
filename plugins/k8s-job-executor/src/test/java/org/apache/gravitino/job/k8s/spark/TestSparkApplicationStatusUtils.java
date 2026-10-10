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
    "DriverStarted, QUEUED",
    "DriverReady, STARTED",
    "InitializedBelowThresholdExecutors, STARTED",
    "RunningHealthy, STARTED",
    "RunningWithPartialCapacity, STARTED",
    "RunningWithBelowThresholdExecutors, STARTED",
    "SomeNewState, QUEUED",
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

    // The application stopped before the deletion.
    GenericKubernetesResource stopped = app("Submitted", "DriverReady", "Failed");
    cancel(stopped);
    JobExecutionInfo info = executionInfoOf(stopped);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, info.status());
    Assertions.assertEquals(timeOf(2), info.finishedAt());
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

  private static void cancel(GenericKubernetesResource app) {
    app.getMetadata()
        .setAnnotations(ImmutableMap.of(K8sJobResourceUtils.ANNOTATION_CANCEL_REQUESTED, "true"));
  }
}
