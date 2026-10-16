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
import org.apache.commons.lang3.StringUtils;
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
      ImmutableSet.of("Submitted", "ScheduledToRestart", "DriverRequested");

  // The operator enters DriverStarted when the driver container is ready, so the job is running
  // from then on. DriverReady only adds that the whole driver pod is ready, which a sidecar that
  // isn't ready can hold back.
  private static final Set<String> STARTED_STATES =
      ImmutableSet.of(
          "DriverStarted",
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
   *       deleted, unless it had stopped before, as the operator recorded it: by an outcome or a
   *       stopped state recorded before the deletion, or whose time is unknown. A driver that
   *       terminated before the deletion doesn't count, as the operator may never record its
   *       outcome once the application is deleted. The cancel mark alone doesn't cancel it: a
   *       cancellation that failed after marking it, for example because the server stopped or a
   *       response was lost, leaves the mark on a job that keeps running, which then keeps the
   *       outcome it reaches.
   *   <li>A SparkApplication being deleted by someone else, for example by its TTL, keeps the
   *       outcome it reached, or fails if it has none.
   *   <li>The stopped states walk back through the state transition history to the last outcome.
   *   <li>A SparkApplication without any status after the timeout fails, for example if its
   *       namespace isn't watched by the operator. One with a status that has no state this version
   *       can read stays queued, even if it was started before, and nothing ends it until the
   *       operator deletes it after its TTL. It means the operator is newer than this version
   *       supports, which must be fixed rather than waited out.
   *   <li>An unknown state, of a newer operator, after an outcome is a stopped state too. Before
   *       any outcome it keeps the job unfinished: started if the application has reached a started
   *       state, queued otherwise.
   * </ul>
   *
   * <p>The operator records a state when it observes it, which can be a while after the driver
   * actually started or finished. So the times are the ones Kubernetes reports for the driver
   * container, in seconds, taken from the given driver pod or from the driver pod status the
   * operator keeps with some states. Without them, the job started when the application first
   * reached a started state, and finished when it reached its outcome. As the driver container
   * status is only known once the job finished, the started time of a running job is refined to the
   * driver's when it finishes. A job that stopped without an outcome finished when its driver
   * container terminated if that is known, else when it entered its stopped state if that is known,
   * or else when it was deleted.
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
    Instant createdAt = parseTime(app.getMetadata().getCreationTimestamp());
    Instant deletedAt = parseTime(app.getMetadata().getDeletionTimestamp());

    ContainerState driverState =
        driver == null ? null : driverContainerState(driver, getDriverContainerName(app));
    Instant driverStartedAt = null;
    Instant driverFinishedAt = null;
    if (driverState != null && driverState.getTerminated() != null) {
      driverStartedAt = parseTime(driverState.getTerminated().getStartedAt());
      driverFinishedAt = parseTime(driverState.getTerminated().getFinishedAt());
    } else if (driverState != null && driverState.getRunning() != null) {
      driverStartedAt = parseTime(driverState.getRunning().getStartedAt());
    }
    Instant startedAt = startedAt(history, driverStartedAt, createdAt);
    // When the application stopped: when the driver container terminated if that is known, even
    // before the operator records an outcome, else when the application reached its outcome, else
    // when it was deleted if it hadn't stopped by then, else when it entered its current state. A
    // deletion time is in whole seconds, so it may read earlier than a started time of the same
    // second, which Gravitino would then drop.
    Instant deletedOrStartedAt =
        deletedAt != null && startedAt != null && deletedAt.isBefore(startedAt)
            ? startedAt
            : deletedAt;
    Instant stoppedAt;
    if (driverFinishedAt != null) {
      stoppedAt = driverFinishedAt;
    } else if (outcome != null) {
      stoppedAt = outcome.driverFinishedAt != null ? outcome.driverFinishedAt : outcome.time;
    } else if (deletedAt != null && !isStopped(current, outcome)) {
      stoppedAt = deletedOrStartedAt;
    } else {
      stoppedAt = current == null ? null : current.time;
    }
    if (stoppedAt == null) {
      // The time of the state it stopped in is unknown, the deletion is the best known one.
      stoppedAt = deletedOrStartedAt;
    }

    JobHandle.Status status;
    Instant finishedAt = null;
    if (K8sJobResourceUtils.isCancelRequested(app)
        && deletedAt != null
        && !stoppedBefore(history, deletedAt)) {
      status = JobHandle.Status.CANCELLED;
      finishedAt = stoppedAt;
    } else if (deletedAt != null || isStopped(current, outcome)) {
      // Stopped, or deleted by someone else than Gravitino, for example by its TTL or kubectl.
      status = outcome == null ? JobHandle.Status.FAILED : statusOf(outcome);
      finishedAt = stoppedAt;
    } else if (current == null) {
      if (hasStatus(app)) {
        // The operator wrote a status this version can't read, for example a newer operator. The
        // application is known to the operator, so the job stays unfinished rather than failing.
        status = JobHandle.Status.QUEUED;
        LOG.warn(
            "Can't read the status of SparkApplication {}, considering it as {}",
            nameOf(app),
            status);
      } else if (createdAt != null && createdAt.plusMillis(noStatusTimeoutMs).isBefore(now)) {
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
    } else if (STARTED_STATES.contains(current.summary)) {
      status = JobHandle.Status.STARTED;
    } else {
      // A state this version doesn't know, of a newer operator, before any outcome. It can't be
      // told whether it is final, so the job stays unfinished: started if it has run, queued
      // otherwise.
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
   * Returns whether the driver of a SparkApplication is still pending or running. It is decided by
   * the driver container, as a sidecar can keep the pod running after the driver exited, and by the
   * pod only if the driver container can't be told.
   *
   * @param app the SparkApplication
   * @param driver the driver pod
   * @return true if the driver hasn't terminated
   */
  public static boolean isDriverRunning(GenericKubernetesResource app, Pod driver) {
    ContainerState state = driverContainerState(driver, getDriverContainerName(app));
    if (state != null) {
      return state.getTerminated() == null;
    }
    String phase = driver.getStatus() == null ? null : driver.getStatus().getPhase();
    return !"Succeeded".equals(phase) && !"Failed".equals(phase);
  }

  /**
   * Returns whether the operator has written a status of the SparkApplication, whether or not this
   * version can read a state from it.
   *
   * @param app the SparkApplication
   * @return true if the application has a non-empty status
   */
  public static boolean hasStatus(GenericKubernetesResource app) {
    Object status = app.getAdditionalProperties().get("status");
    return status instanceof Map && !((Map<?, ?>) status).isEmpty();
  }

  /**
   * Returns whether the operator has reported a state of the SparkApplication that can be read.
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
    return isStopped(
        history.isEmpty() ? null : history.get(history.size() - 1), lastOutcome(history));
  }

  /**
   * Returns whether the application stopped: its current state is an outcome or a stopped state, or
   * a state this version doesn't know that follows an outcome. A known state after an outcome means
   * the application runs again.
   */
  private static boolean isStopped(@Nullable State current, @Nullable State outcome) {
    if (current == null) {
      return false;
    }
    boolean running =
        QUEUED_STATES.contains(current.summary) || STARTED_STATES.contains(current.summary);
    return isOutcome(current)
        || STOPPED_STATES.contains(current.summary)
        || (!running && outcome != null);
  }

  /**
   * Returns whether the application had reached an outcome or a stopped state before it was
   * deleted. A deletion time is in whole seconds, so a state of the same second counts as after. A
   * state whose time is unknown counts as before, like {@link #isStopped(State, State)}, which
   * doesn't need the times either.
   */
  private static boolean stoppedBefore(List<State> history, Instant deletedAt) {
    return history.stream()
        .anyMatch(
            s ->
                (isOutcome(s) || STOPPED_STATES.contains(s.summary))
                    && (s.time == null || s.time.isBefore(deletedAt)));
  }

  private static boolean isOutcome(State state) {
    return SUCCEEDED_STATE.equals(state.summary) || FAILED_STATES.contains(state.summary);
  }

  private static JobHandle.Status statusOf(State outcome) {
    return SUCCEEDED_STATE.equals(outcome.summary)
        ? JobHandle.Status.SUCCEEDED
        : JobHandle.Status.FAILED;
  }

  /**
   * Returns when the job started: when the driver container did, by the driver pod or else by the
   * driver status the operator observed, or else when the application reached a started state.
   */
  @Nullable
  private static Instant startedAt(
      List<State> history, @Nullable Instant driverStartedAt, @Nullable Instant createdAt) {
    Instant containerStartedAt =
        driverStartedAt != null
            ? driverStartedAt
            : history.stream()
                .map(s -> s.driverStartedAt)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder())
                .orElse(null);
    // A container that failed to start can report a start time of the epoch. A driver can't have
    // started before its application was created.
    if (containerStartedAt != null
        && (createdAt == null || !containerStartedAt.isBefore(createdAt))) {
      return containerStartedAt;
    }
    return history.stream()
        .filter(s -> STARTED_STATES.contains(s.summary))
        .findFirst()
        .map(s -> s.time)
        .orElse(null);
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
    String driverContainer = getDriverContainerName(app);

    Object transitions = ((Map<String, Object>) status).get("stateTransitionHistory");
    if (transitions instanceof Map) {
      ((Map<String, Object>) transitions)
          .forEach(
              (id, state) -> {
                State parsed = parseState(id, state, driverContainer);
                if (parsed != null) {
                  history.add(parsed);
                }
              });
      history.sort(Comparator.comparingLong(s -> s.id));
    }

    State current =
        parseState(null, ((Map<String, Object>) status).get("currentState"), driverContainer);
    if (current != null
        && (history.isEmpty()
            || !current.summary.equals(history.get(history.size() - 1).summary))) {
      history.add(current);
    }
    return history;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static State parseState(
      @Nullable String id, @Nullable Object state, String driverContainer) {
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

    Map<String, Object> driverState =
        observedDriverContainerState(fields.get("lastObservedDriverStatus"), driverContainer);
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

  /**
   * Returns the name of the driver container of a SparkApplication: the one the job names for its
   * driver pod template, or else the one Spark gives it.
   *
   * @param app the SparkApplication
   * @return the name of the driver container
   */
  public static String getDriverContainerName(GenericKubernetesResource app) {
    Map<String, Object> spec = mapOf(app.getAdditionalProperties().get("spec"));
    Map<String, Object> sparkConf = spec == null ? null : mapOf(spec.get("sparkConf"));
    Object name =
        sparkConf == null ? null : sparkConf.get(SparkApplicationUtils.SPARK_DRIVER_CONTAINER_NAME);
    return name instanceof String && StringUtils.isNotBlank((String) name)
        ? (String) name
        : SparkApplicationUtils.DRIVER_CONTAINER_NAME;
  }

  /** Returns the state of the driver container of the driver pod. */
  @Nullable
  private static ContainerState driverContainerState(Pod driver, String driverContainer) {
    List<ContainerStatus> containers =
        driver.getStatus() == null ? null : driver.getStatus().getContainerStatuses();
    if (containers == null) {
      return null;
    }
    for (ContainerStatus container : containers) {
      if (driverContainer.equals(container.getName())) {
        return container.getState();
      }
    }
    // The only container is the driver, even if a pod template renamed it.
    return containers.size() == 1 ? containers.get(0).getState() : null;
  }

  /** Returns the state of the driver container in the observed status of the driver pod. */
  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> observedDriverContainerState(
      @Nullable Object podStatus, String driverContainer) {
    Map<String, Object> status = mapOf(podStatus);
    Object containers = status == null ? null : status.get("containerStatuses");
    if (!(containers instanceof List)) {
      return null;
    }

    List<Object> containerStatuses = (List<Object>) containers;
    for (Object container : containerStatuses) {
      Map<String, Object> fields = mapOf(container);
      if (fields != null && driverContainer.equals(fields.get("name"))) {
        return mapOf(fields.get("state"));
      }
    }
    // The only container is the driver, even if a pod template renamed it.
    return containerStatuses.size() == 1 ? stateOf(containerStatuses.get(0)) : null;
  }

  @Nullable
  private static Map<String, Object> stateOf(@Nullable Object container) {
    Map<String, Object> fields = mapOf(container);
    return fields == null ? null : mapOf(fields.get("state"));
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> mapOf(@Nullable Object value) {
    return value instanceof Map ? (Map<String, Object>) value : null;
  }

  @Nullable
  private static Instant timeOf(@Nullable Object value) {
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
