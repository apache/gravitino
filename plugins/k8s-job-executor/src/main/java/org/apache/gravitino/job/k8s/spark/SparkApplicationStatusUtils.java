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

import com.google.common.collect.ImmutableSet;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.k8s.K8sJobResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps the state of a SparkApplication of apache/spark-kubernetes-operator to the Gravitino job
 * execution info. The state names are the operator's {@code ApplicationStateSummary}.
 */
public final class SparkApplicationStatusUtils {

  /**
   * A state of the SparkApplication, an entry of its state transition history. It may carry the
   * status of the driver pod the operator observed when entering the state.
   */
  private static final class State {

    private final long id;

    private final String summary;

    @Nullable private final Instant time;

    @Nullable private final Instant driverStartedAt;

    @Nullable private final Instant driverFinishedAt;

    private State(
        long id,
        String summary,
        @Nullable Instant time,
        @Nullable Instant driverStartedAt,
        @Nullable Instant driverFinishedAt) {
      this.id = id;
      this.summary = summary;
      this.time = time;
      this.driverStartedAt = driverStartedAt;
      this.driverFinishedAt = driverFinishedAt;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SparkApplicationStatusUtils.class);

  private static final Set<String> QUEUED_STATES =
      ImmutableSet.of("Submitted", "ScheduledToRestart", "DriverRequested", "DriverStarted");

  private static final Set<String> STARTED_STATES =
      ImmutableSet.of(
          "DriverReady",
          "InitializedBelowThresholdExecutors",
          "RunningHealthy",
          "RunningWithPartialCapacity",
          "RunningWithBelowThresholdExecutors");

  private static final String SUCCEEDED_STATE = "Succeeded";

  private static final Set<String> FAILED_STATES =
      ImmutableSet.of(
          "Failed",
          "SchedulingFailure",
          "DriverEvicted",
          "DriverStartTimedOut",
          "ExecutorsStartTimedOut",
          "DriverReadyTimedOut");

  // The last states of every application, which don't tell success from failure.
  private static final Set<String> STOPPED_STATES =
      ImmutableSet.of("ResourceReleased", "TerminatedWithoutReleaseResources");

  private SparkApplicationStatusUtils() {}

  /**
   * Maps the SparkApplication to the job execution info.
   *
   * <ul>
   *   <li>A SparkApplication that Gravitino requested to cancel is cancelled once it is being
   *       deleted or has stopped.
   *   <li>A SparkApplication being deleted by someone else, for example by its TTL, keeps the
   *       outcome it reached, or fails if it has none.
   *   <li>The stopped states walk back through the state transition history to the last outcome.
   *   <li>A SparkApplication without any status after the timeout fails, for example if its
   *       namespace isn't watched by the operator.
   *   <li>An unknown state, of a newer operator, keeps the job unfinished: started if the
   *       application has reached a started state, queued otherwise.
   * </ul>
   *
   * <p>The operator records a state when it observes it, which can be a while after the driver
   * actually started or finished. So the times are the ones Kubernetes reports for the driver
   * container, in seconds, taken from the given driver pod or from the driver pod status the
   * operator keeps with some states. Without them, the job started when the application first
   * reached a started state, and finished when it reached its outcome. As the driver container
   * status is only known once the job finished, the started time of a running job is refined to the
   * driver's when it finishes.
   *
   * @param app the SparkApplication
   * @param driver the driver pod, or null if unknown
   * @param now the current time
   * @param noStatusTimeoutMs the time after which a SparkApplication without status fails
   * @return the job execution info
   */
  public static JobExecutionInfo executionInfoOf(
      GenericKubernetesResource app, @Nullable Pod driver, Instant now, long noStatusTimeoutMs) {
    List<State> history = stateHistory(app);
    State current = history.isEmpty() ? null : history.get(history.size() - 1);
    State outcome = lastOutcome(history);
    ContainerState driverState = driver == null ? null : driverContainerState(driver);
    Instant driverStartedAt = null;
    Instant driverFinishedAt = null;
    if (driverState != null && driverState.getTerminated() != null) {
      driverStartedAt = parseTime(driverState.getTerminated().getStartedAt());
      driverFinishedAt = parseTime(driverState.getTerminated().getFinishedAt());
    } else if (driverState != null && driverState.getRunning() != null) {
      driverStartedAt = parseTime(driverState.getRunning().getStartedAt());
    }
    Instant startedAt = driverStartedAt != null ? driverStartedAt : startedAt(history);
    Instant deletedAt = parseTime(app.getMetadata().getDeletionTimestamp());

    JobHandle.Status status;
    Instant finishedAt = null;
    if (K8sJobResourceUtils.isCancelRequested(app) && (deletedAt != null || isStopped(current))) {
      status = JobHandle.Status.CANCELLED;
      finishedAt = outcome != null ? finishedAt(outcome, driverFinishedAt) : deletedAt;
    } else if (deletedAt != null || (current != null && STOPPED_STATES.contains(current.summary))) {
      // Deleted by someone else than Gravitino, for example by its TTL or kubectl, or stopped.
      status = outcome == null ? JobHandle.Status.FAILED : statusOf(outcome);
      finishedAt = outcome != null ? finishedAt(outcome, driverFinishedAt) : deletedAt;
    } else if (current == null) {
      Instant createdAt = parseTime(app.getMetadata().getCreationTimestamp());
      if (createdAt != null && createdAt.plusMillis(noStatusTimeoutMs).isBefore(now)) {
        LOG.warn(
            "SparkApplication {} has no status after {} ms, is its namespace watched by the Spark"
                + " operator? Considering it as failed",
            nameOf(app),
            noStatusTimeoutMs);
        status = JobHandle.Status.FAILED;
      } else {
        status = JobHandle.Status.QUEUED;
      }
    } else if (QUEUED_STATES.contains(current.summary)) {
      status = JobHandle.Status.QUEUED;
    } else if (isOutcome(current)) {
      status = statusOf(current);
      finishedAt = finishedAt(current, driverFinishedAt);
    } else if (STARTED_STATES.contains(current.summary)) {
      status = JobHandle.Status.STARTED;
    } else {
      // A state this version doesn't know, of a newer operator. It can't be told whether it is
      // final, so the job stays unfinished: started if it has run, queued otherwise.
      status =
          history.stream().anyMatch(s -> STARTED_STATES.contains(s.summary))
              ? JobHandle.Status.STARTED
              : JobHandle.Status.QUEUED;
      LOG.warn(
          "Unknown state {} of SparkApplication {}, considering it as {}",
          current.summary,
          nameOf(app),
          status);
    }

    return JobExecutionInfo.builder()
        .withStatus(status)
        .withStartedAt(startedAt)
        .withFinishedAt(finishedAt)
        .build();
  }

  /**
   * Returns whether the operator has reported any state of the SparkApplication.
   *
   * @param app the SparkApplication
   * @return true if the application has a state
   */
  public static boolean hasState(GenericKubernetesResource app) {
    return !stateHistory(app).isEmpty();
  }

  /**
   * Returns whether the SparkApplication has stopped running, successfully or not.
   *
   * @param app the SparkApplication
   * @return true if the application has reached an outcome or a stopped state
   */
  public static boolean isStopped(GenericKubernetesResource app) {
    List<State> history = stateHistory(app);
    return isStopped(history.isEmpty() ? null : history.get(history.size() - 1));
  }

  private static boolean isStopped(@Nullable State state) {
    return state != null && (isOutcome(state) || STOPPED_STATES.contains(state.summary));
  }

  private static boolean isOutcome(State state) {
    return SUCCEEDED_STATE.equals(state.summary) || FAILED_STATES.contains(state.summary);
  }

  private static JobHandle.Status statusOf(State outcome) {
    return SUCCEEDED_STATE.equals(outcome.summary)
        ? JobHandle.Status.SUCCEEDED
        : JobHandle.Status.FAILED;
  }

  @Nullable
  private static Instant startedAt(List<State> history) {
    Instant driverStartedAt =
        history.stream()
            .map(s -> s.driverStartedAt)
            .filter(Objects::nonNull)
            .min(Comparator.naturalOrder())
            .orElse(null);
    if (driverStartedAt != null) {
      return driverStartedAt;
    }
    return history.stream()
        .filter(s -> STARTED_STATES.contains(s.summary))
        .findFirst()
        .map(s -> s.time)
        .orElse(null);
  }

  @Nullable
  private static Instant finishedAt(State outcome, @Nullable Instant driverFinishedAt) {
    if (driverFinishedAt != null) {
      return driverFinishedAt;
    }
    return outcome.driverFinishedAt != null ? outcome.driverFinishedAt : outcome.time;
  }

  @Nullable
  private static State lastOutcome(List<State> history) {
    for (int i = history.size() - 1; i >= 0; i--) {
      if (isOutcome(history.get(i))) {
        return history.get(i);
      }
    }
    return null;
  }

  /**
   * Returns the state transition history of the SparkApplication, oldest first. It ends with the
   * current state, which is also recorded on its own in case the history is trimmed.
   */
  @SuppressWarnings("unchecked")
  private static List<State> stateHistory(GenericKubernetesResource app) {
    List<State> history = new ArrayList<>();
    Object status = app.getAdditionalProperties().get("status");
    if (!(status instanceof Map)) {
      return history;
    }

    Object transitions = ((Map<String, Object>) status).get("stateTransitionHistory");
    if (transitions instanceof Map) {
      ((Map<String, Object>) transitions)
          .forEach(
              (id, state) -> {
                State parsed = parseState(id, state);
                if (parsed != null) {
                  history.add(parsed);
                }
              });
      history.sort(Comparator.comparingLong(s -> s.id));
    }

    State current = parseState(null, ((Map<String, Object>) status).get("currentState"));
    if (current != null
        && (history.isEmpty()
            || !current.summary.equals(history.get(history.size() - 1).summary))) {
      history.add(current);
    }
    return history;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static State parseState(@Nullable String id, Object state) {
    if (!(state instanceof Map)) {
      return null;
    }
    Map<String, Object> fields = (Map<String, Object>) state;
    Object summary = fields.get("currentStateSummary");
    if (!(summary instanceof String)) {
      return null;
    }
    long order;
    try {
      order = id == null ? Long.MAX_VALUE : Long.parseLong(id);
    } catch (NumberFormatException e) {
      return null;
    }

    Map<String, Object> driverState = driverContainerState(fields.get("lastObservedDriverStatus"));
    return new State(
        order,
        (String) summary,
        timeOf(fields.get("lastTransitionTime")),
        driverState == null ? null : driverStartedAt(driverState),
        driverState == null ? null : driverFinishedAt(driverState));
  }

  @Nullable
  private static Instant driverStartedAt(Map<String, Object> driverState) {
    Map<String, Object> terminated = mapOf(driverState.get("terminated"));
    if (terminated != null) {
      return timeOf(terminated.get("startedAt"));
    }
    Map<String, Object> running = mapOf(driverState.get("running"));
    return running == null ? null : timeOf(running.get("startedAt"));
  }

  @Nullable
  private static Instant driverFinishedAt(Map<String, Object> driverState) {
    Map<String, Object> terminated = mapOf(driverState.get("terminated"));
    return terminated == null ? null : timeOf(terminated.get("finishedAt"));
  }

  /** Returns the state of the driver container of the driver pod. */
  @Nullable
  private static ContainerState driverContainerState(Pod driver) {
    List<ContainerStatus> containers =
        driver.getStatus() == null ? null : driver.getStatus().getContainerStatuses();
    if (containers == null) {
      return null;
    }
    for (ContainerStatus container : containers) {
      if (SparkApplicationUtils.DRIVER_CONTAINER_NAME.equals(container.getName())) {
        return container.getState();
      }
    }
    // The only container is the driver, even if a pod template renamed it.
    return containers.size() == 1 ? containers.get(0).getState() : null;
  }

  /** Returns the state of the driver container in the observed status of the driver pod. */
  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> driverContainerState(Object podStatus) {
    Map<String, Object> status = mapOf(podStatus);
    Object containers = status == null ? null : status.get("containerStatuses");
    if (!(containers instanceof List)) {
      return null;
    }

    List<Object> containerStatuses = (List<Object>) containers;
    for (Object container : containerStatuses) {
      Map<String, Object> fields = mapOf(container);
      if (fields != null
          && SparkApplicationUtils.DRIVER_CONTAINER_NAME.equals(fields.get("name"))) {
        return mapOf(fields.get("state"));
      }
    }
    // The only container is the driver, even if a pod template renamed it.
    return containerStatuses.size() == 1 ? stateOf(containerStatuses.get(0)) : null;
  }

  @Nullable
  private static Map<String, Object> stateOf(Object container) {
    Map<String, Object> fields = mapOf(container);
    return fields == null ? null : mapOf(fields.get("state"));
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> mapOf(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : null;
  }

  @Nullable
  private static Instant timeOf(Object value) {
    return value instanceof String ? parseTime((String) value) : null;
  }

  @Nullable
  private static Instant parseTime(@Nullable String time) {
    if (time == null) {
      return null;
    }
    try {
      return Instant.parse(time);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static String nameOf(GenericKubernetesResource app) {
    ObjectMeta metadata = app.getMetadata();
    return metadata.getNamespace() + "/" + metadata.getName();
  }
}
