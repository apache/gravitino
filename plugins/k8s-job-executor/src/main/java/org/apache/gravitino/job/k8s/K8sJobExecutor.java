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
package org.apache.gravitino.job.k8s;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.time.Clock;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.job.JobContext;
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.job.k8s.spark.SparkApplicationStatusUtils;
import org.apache.gravitino.job.k8s.spark.SparkApplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A job executor that runs jobs on Kubernetes. Spark jobs run as SparkApplication resources of <a
 * href="https://github.com/apache/spark-kubernetes-operator">apache/spark-kubernetes-operator</a>
 * 0.9.x, shell jobs aren't supported yet.
 *
 * <p>All the job state lives in Kubernetes, so every Gravitino server can query and cancel any job.
 * The job execution id is {@code <cluster>/<kind>/<namespace>/<name>}, see {@link
 * K8sJobExecutionId}.
 */
public class K8sJobExecutor implements JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(K8sJobExecutor.class);

  // Not defined by HttpURLConnection.
  private static final int HTTP_UNPROCESSABLE_ENTITY = 422;

  // The responses of the API server that reject a resource as it is, so creating it again can't
  // succeed: bad request, forbidden, not found (the namespace or the SparkApplication CRD), and
  // unprocessable entity.
  private static final Set<Integer> REJECTED_REQUEST_CODES =
      ImmutableSet.of(
          HttpURLConnection.HTTP_BAD_REQUEST,
          HttpURLConnection.HTTP_FORBIDDEN,
          HttpURLConnection.HTTP_NOT_FOUND,
          HTTP_UNPROCESSABLE_ENTITY);

  // How long the status queries fail right away after the API server couldn't be reached.
  // Gravitino queries the jobs one after another, and each query would otherwise wait for the
  // connection to time out again.
  @VisibleForTesting static final long UNREACHABLE_BACKOFF_MS = 10_000L;

  // How many times a change of a SparkApplication is tried again after it changed meanwhile.
  private static final int UPDATE_CONFLICT_RETRIES = 3;

  private final Clock clock;

  // The failure of the last status query that couldn't reach the API server, and when it failed.
  // Reset once the backoff is over.
  @Nullable private volatile KubernetesClientException unreachableFailure;

  private volatile long unreachableAtMs;

  private K8sJobExecutorConfigs configs;

  private KubernetesClient client;

  private NamespacedResourceCache<GenericKubernetesResource> sparkApplicationCache;

  /** Creates the job executor, which Gravitino initializes with {@link #initialize(Map)}. */
  public K8sJobExecutor() {
    this(Clock.systemUTC());
  }

  @VisibleForTesting
  K8sJobExecutor(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void initialize(Map<String, String> configs) {
    K8sJobExecutorConfigs parsed = new K8sJobExecutorConfigs(configs);
    initialize(parsed, K8sClientUtils.createClient(parsed));
  }

  @VisibleForTesting
  void initialize(K8sJobExecutorConfigs configs, KubernetesClient client) {
    this.configs = configs;
    this.client = client;
    this.sparkApplicationCache =
        new NamespacedResourceCache<>(
            configs.statusCacheTtlMs(),
            namespace ->
                getSparkApplicationClient(namespace)
                    .withLabel(
                        K8sJobResourceUtils.LABEL_MANAGED_BY,
                        K8sJobResourceUtils.MANAGED_BY_GRAVITINO)
                    .list()
                    .getItems(),
            // Getting the SparkApplication itself couldn't reach the API server either.
            e ->
                e instanceof KubernetesClientException
                    && isUnreachable((KubernetesClientException) e),
            clock::millis);
    LOG.info(
        "Initialized the k8s job executor for cluster {} at {}, default namespace {}",
        configs.cluster(),
        client.getMasterUrl(),
        configs.namespace());
  }

  @Override
  public String submitJob(JobContext context, JobTemplate jobTemplate) {
    if (!(jobTemplate instanceof SparkJobTemplate)) {
      throw new IllegalArgumentException(
          String.format(
              "The k8s job executor only supports Spark jobs, but job template %s is a %s job",
              jobTemplate.name(), jobTemplate.jobType()));
    }

    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(
            context, (SparkJobTemplate) jobTemplate, configs);
    String namespace = app.getMetadata().getNamespace();
    String name = app.getMetadata().getName();
    K8sJobExecutionId jobExecutionId =
        K8sJobExecutionId.of(
            configs.cluster(), K8sJobExecutionId.Kind.SPARK_APPLICATION, namespace, name);
    try {
      getSparkApplicationClient(namespace).resource(app).create();
    } catch (KubernetesClientException e) {
      if (isUnreachable(e)) {
        // The SparkApplication may have been created although no response came back. Gravitino
        // records no job for a failed submission, so delete it, otherwise it would run unknown.
        deleteUnsubmittedSparkApplication(jobExecutionId, context.jobId());
        throw e;
      }
      if (REJECTED_REQUEST_CODES.contains(e.getCode())) {
        // The cluster rejects the job as it is, for example its namespace doesn't exist or isn't
        // allowed, so the caller gets the reason rather than an internal error.
        throw new IllegalArgumentException(
            String.format(
                "Kubernetes rejected the SparkApplication %s/%s of job template %s: %s",
                namespace, name, jobTemplate.name(), e.getMessage()),
            e);
      }
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      // Created by an earlier attempt to submit the same job. Never adopt a resource of another
      // job, for example one left by another Gravitino deployment sharing the namespace, nor one
      // that is being deleted.
      GenericKubernetesResource existing =
          getSparkApplicationClient(namespace).withName(name).get();
      if (existing == null
          || existing.getMetadata().getDeletionTimestamp() != null
          || !K8sJobResourceUtils.isOfJob(existing, context.jobId())) {
        throw new IllegalStateException(
            String.format(
                "SparkApplication %s/%s already exists and can't be used for job %s. If it belongs"
                    + " to another Gravitino deployment sharing the namespace, use a different %s"
                    + " for each of them",
                namespace, name, context.jobId(), K8sJobExecutorConfigs.NAME_PREFIX),
            e);
      }
    }

    LOG.info("Submitted job {} as {}", context.jobId(), jobExecutionId);
    return jobExecutionId.toString();
  }

  @Override
  public JobExecutionInfo getJobExecutionInfo(String jobId) throws NoSuchJobException {
    K8sJobExecutionId id = parseJobExecutionId(jobId);
    checkApiServerReachable();
    try {
      return getSparkApplicationExecutionInfo(id, jobId);
    } catch (KubernetesClientException e) {
      if (isUnreachable(e)) {
        unreachableAtMs = clock.millis();
        unreachableFailure = e;
      }
      throw e;
    }
  }

  @Override
  public void cancelJob(String jobId) throws NoSuchJobException {
    K8sJobExecutionId id = parseJobExecutionId(jobId);
    // Mark the cancellation before deleting, so that a status query on any server tells a
    // cancelled job from one deleted by someone else. A job that has stopped isn't marked, so
    // that a finished job isn't reported as cancelled. The mark is only written if the job hasn't
    // changed since it was checked, so a job that stops meanwhile is checked again.
    GenericKubernetesResource app =
        updateSparkApplication(
                id,
                current -> {
                  if (SparkApplicationStatusUtils.isStopped(current)) {
                    return false;
                  }
                  setCancelRequested(current, true);
                  return true;
                })
            .orElseThrow(
                () -> new NoSuchJobException("SparkApplication of job %s doesn't exist", jobId));
    if (SparkApplicationStatusUtils.isStopped(app)) {
      return;
    }

    try {
      deleteSparkApplication(id);
    } catch (KubernetesClientException e) {
      // The deletion may have taken effect although its response was lost. Then the job is being
      // cancelled, and keeps its mark.
      AtomicBoolean deleting = new AtomicBoolean();
      try {
        Optional<GenericKubernetesResource> latest =
            updateSparkApplication(
                id,
                current -> {
                  deleting.set(current.getMetadata().getDeletionTimestamp() != null);
                  if (deleting.get()) {
                    return false;
                  }
                  // The job keeps running and the caller is told the cancellation failed, so
                  // remove the mark, otherwise the job would be reported as cancelled if someone
                  // else deleted it while it runs. A mark that can't be removed is harmless
                  // otherwise: without a deletion, the job keeps the outcome it reaches.
                  setCancelRequested(current, false);
                  return true;
                });
        deleting.compareAndSet(false, !latest.isPresent());
      } catch (RuntimeException removal) {
        e.addSuppressed(removal);
        throw e;
      }
      if (!deleting.get()) {
        throw e;
      }
      LOG.warn("The deletion of the SparkApplication of job {} took effect anyway", jobId, e);
    }
    LOG.info("Requested to cancel job {}", jobId);
  }

  @Override
  public List<String> getJobStdout(String jobId, int maxLines, int maxBytes) {
    try {
      K8sJobExecutionId id = parseJobExecutionId(jobId);
      // The SparkApplication tells the driver pod apart from other pods with the same labels, and
      // names the driver container. It may already be gone, the driver pod is retained longer.
      GenericKubernetesResource app =
          getSparkApplicationClient(id.namespace()).withName(id.name()).get();
      if (app != null && !K8sJobResourceUtils.isJobResource(app)) {
        // The name is used by a resource that isn't the job's, whose driver log isn't the job's.
        return ImmutableList.of();
      }
      Optional<Pod> driver = getDriverPod(id, app);
      if (!driver.isPresent()) {
        return ImmutableList.of();
      }

      // Kubernetes can only limit the bytes from the head of the log, so read the last lines and
      // keep their tail. So the lines bound the bytes read, and maxBytes only the bytes kept.
      PodResource pod =
          client.pods().inNamespace(id.namespace()).withName(driver.get().getMetadata().getName());
      // The container must be named if the driver pod has others, for example a sidecar.
      List<Container> containers =
          driver.get().getSpec() == null ? null : driver.get().getSpec().getContainers();
      String driverContainer =
          app == null
              ? SparkApplicationUtils.DRIVER_CONTAINER_NAME
              : SparkApplicationStatusUtils.getDriverContainerName(app);
      boolean nameContainer = containers != null && containers.size() > 1;
      try (InputStream log =
          (nameContainer ? pod.inContainer(driverContainer) : pod)
              .tailingLines(maxLines)
              .getLogInputStream()) {
        return PodLogUtils.readLastLines(log, maxLines, maxBytes);
      }
    } catch (KubernetesClientException e) {
      if (e.getCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        // For example, the driver container hasn't started yet.
        LOG.debug("No driver log of job {} to read yet: {}", jobId, e.getMessage());
      } else {
        LOG.warn("Failed to read the driver log of job {}", jobId, e);
      }
      return ImmutableList.of();
    } catch (RuntimeException | IOException e) {
      // The output is best effort and must never fail the caller.
      LOG.warn("Failed to read the driver log of job {}", jobId, e);
      return ImmutableList.of();
    }
  }

  /**
   * Returns an empty list: Kubernetes merges the standard error into the standard output of a pod,
   * so it is returned by {@link #getJobStdout(String, int, int)}.
   */
  @Override
  public List<String> getJobStderr(String jobId, int maxLines, int maxBytes) {
    return ImmutableList.of();
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  /**
   * Returns whether a request failed because the API server couldn't be reached or didn't answer,
   * rather than because the API server refused or failed the request.
   *
   * @param e the failure of the request
   * @return true if the API server gave no response
   */
  @VisibleForTesting
  static boolean isUnreachable(KubernetesClientException e) {
    if (e.getStatus() != null) {
      return false;
    }
    boolean ioFailure = false;
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      // A response that isn't JSON, for example the login page of a proxy, did come back.
      if (cause instanceof JsonProcessingException) {
        return false;
      }
      ioFailure |= cause instanceof IOException;
    }
    return ioFailure;
  }

  /**
   * Fails right away if the API server couldn't be reached a moment ago, so that querying the
   * status of many jobs doesn't wait for the connection to time out once per job.
   */
  private void checkApiServerReachable() {
    KubernetesClientException failure = unreachableFailure;
    if (failure == null) {
      return;
    }
    long remainingMs = unreachableAtMs + UNREACHABLE_BACKOFF_MS - clock.millis();
    if (remainingMs <= 0) {
      unreachableFailure = null;
      return;
    }
    throw new KubernetesClientException(
        String.format(
            "The API server of cluster %s couldn't be reached a moment ago, the status of its jobs"
                + " isn't queried again for %s ms",
            configs.cluster(), remainingMs),
        failure);
  }

  private JobExecutionInfo getSparkApplicationExecutionInfo(K8sJobExecutionId id, String jobId) {
    Optional<GenericKubernetesResource> cached =
        sparkApplicationCache.find(id.namespace(), id.name());
    GenericKubernetesResource app =
        (cached.isPresent() ? cached : getSparkApplication(id))
            .filter(K8sJobResourceUtils::isJobResource)
            .orElseThrow(
                () -> new NoSuchJobException("SparkApplication of job %s doesn't exist", jobId));
    return getExecutionInfo(id, jobId, app);
  }

  private JobExecutionInfo getExecutionInfo(
      K8sJobExecutionId id, String jobId, GenericKubernetesResource app) {
    // The operator observes the driver finishing a while after it does, so a finished job takes
    // its times from the driver pod, which is retained after the job finishes. So does a job whose
    // SparkApplication is being deleted, for example a cancelled one, whose driver may have
    // terminated before the operator recorded any outcome.
    Pod driver = null;
    KubernetesClientException driverFailure = null;
    if (SparkApplicationStatusUtils.isStopped(app)
        || app.getMetadata().getDeletionTimestamp() != null) {
      try {
        driver = getDriverPod(id, app).orElse(null);
      } catch (KubernetesClientException e) {
        driverFailure = e;
      }
    }
    JobExecutionInfo info =
        SparkApplicationStatusUtils.executionInfoOf(
            app, driver, clock.instant(), configs.noStatusTimeoutMs());
    if (info.status() == JobHandle.Status.FAILED
        && !SparkApplicationStatusUtils.hasStatus(app)
        && app.getMetadata().getDeletionTimestamp() == null) {
      return getExecutionInfoWithoutStatus(id, jobId, info);
    }
    if (info.status() == JobHandle.Status.FAILED && driverFailure != null) {
      // Whether the driver still runs can't be told, so the job isn't failed yet: a later query
      // checks its driver again.
      throw driverFailure;
    }
    if (driverFailure != null) {
      LOG.warn("Failed to get the driver pod of job {}", jobId, driverFailure);
    }
    if (info.status() == JobHandle.Status.FAILED
        && driver != null
        && SparkApplicationStatusUtils.isDriverRunning(app, driver)) {
      // The operator retains the driver pod of a stopped application, even one that is still
      // pending or running after the application timed out starting. Delete it, otherwise it
      // could still run the job after it failed. A failure to delete it fails this status query,
      // so that the job only fails once the pod is deleted, by a later query.
      LOG.warn(
          "Deleting the driver pod {} of failed job {}, as it hasn't terminated",
          driver.getMetadata().getName(),
          jobId);
      client.pods().inNamespace(id.namespace()).withName(driver.getMetadata().getName()).delete();
    }
    return info;
  }

  /**
   * Parses the job execution id of a job of the connected cluster. A job of another cluster isn't
   * reported as missing, which would end it while it may still run, for example after the cluster
   * is renamed: the query fails, and the job is left as it is.
   */
  private K8sJobExecutionId parseJobExecutionId(String jobId) throws NoSuchJobException {
    K8sJobExecutionId id = K8sJobExecutionId.parse(jobId);
    if (!configs.cluster().equals(id.cluster())) {
      throw new IllegalStateException(
          String.format(
              "Job %s runs in cluster %s, but the k8s job executor is connected to cluster %s",
              jobId, id.cluster(), configs.cluster()));
    }
    return id;
  }

  /**
   * Returns the execution info of a job whose SparkApplication got no status from the operator in
   * time, which is failed. The SparkApplication is deleted first, otherwise the operator would
   * still run it once it comes back, with the job already failed and out of Gravitino's reach. A
   * failure to delete it fails the status query, so that the job only fails once it is deleted, by
   * a later query.
   */
  private JobExecutionInfo getExecutionInfoWithoutStatus(
      K8sJobExecutionId id, String jobId, JobExecutionInfo failed) {
    // The SparkApplication may come from the cache: it may have got a status since, or be being
    // deleted, for example by a cancellation on another server. Then it is mapped as usual. If it
    // is gone meanwhile, the job is missing rather than failed, which settles a cancelling job as
    // cancelled.
    GenericKubernetesResource latest =
        getSparkApplication(id)
            .orElseThrow(
                () -> new NoSuchJobException("SparkApplication of job %s doesn't exist", jobId));
    if (SparkApplicationStatusUtils.hasStatus(latest)
        || latest.getMetadata().getDeletionTimestamp() != null) {
      sparkApplicationCache.invalidate(id.namespace());
      return getExecutionInfo(id, jobId, latest);
    }
    deleteSparkApplication(id);
    return failed;
  }

  /**
   * Deletes the SparkApplication of a job whose submission failed without telling whether it was
   * created, if it was. Only a SparkApplication of this job is deleted, and any failure is ignored:
   * the submission fails anyway.
   */
  private void deleteUnsubmittedSparkApplication(K8sJobExecutionId id, long jobId) {
    try {
      Optional<GenericKubernetesResource> app = getSparkApplication(id);
      if (app.isPresent() && K8sJobResourceUtils.isOfJob(app.get(), jobId)) {
        deleteSparkApplication(id);
        LOG.info("Deleted the SparkApplication of job {}, whose submission failed", jobId);
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to clean up the SparkApplication {} of job {}, whose submission failed",
          id,
          jobId,
          e);
    }
  }

  /**
   * Returns the SparkApplication of a job, if it exists. A resource with its name that isn't a job
   * resource of Gravitino, for example one created by another system, isn't.
   */
  private Optional<GenericKubernetesResource> getSparkApplication(K8sJobExecutionId id) {
    return Optional.ofNullable(getSparkApplicationClient(id.namespace()).withName(id.name()).get())
        .filter(K8sJobResourceUtils::isJobResource);
  }

  /**
   * Changes the SparkApplication of a job, only if it hasn't changed since it was read, and reads
   * it again to decide the change again otherwise.
   *
   * @param id the job execution id
   * @param change changes the SparkApplication in place, and returns whether it changed it
   * @return the SparkApplication the change was decided on, or empty if it doesn't exist
   */
  private Optional<GenericKubernetesResource> updateSparkApplication(
      K8sJobExecutionId id, Predicate<GenericKubernetesResource> change) {
    for (int attempt = 0; ; attempt++) {
      Optional<GenericKubernetesResource> app = getSparkApplication(id);
      if (!app.isPresent() || !change.test(app.get())) {
        return app;
      }
      try {
        // An update carries the resource version that was read, so it fails if the
        // SparkApplication has changed since.
        getSparkApplicationClient(id.namespace()).resource(app.get()).update();
        sparkApplicationCache.invalidate(id.namespace());
        return app;
      } catch (KubernetesClientException e) {
        if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          return Optional.empty();
        }
        if (e.getCode() != HttpURLConnection.HTTP_CONFLICT || attempt >= UPDATE_CONFLICT_RETRIES) {
          throw e;
        }
      }
    }
  }

  /**
   * Returns the newest driver pod of a SparkApplication. The operator names the driver pod after
   * the Spark app id, which it may hash-truncate, so it is looked up by the operator's labels, and
   * also by its owner, the SparkApplication, when that is known.
   */
  private Optional<Pod> getDriverPod(
      K8sJobExecutionId id, @Nullable GenericKubernetesResource app) {
    String appUid = app == null ? null : app.getMetadata().getUid();
    return client
        .pods()
        .inNamespace(id.namespace())
        .withLabel(SparkApplicationUtils.LABEL_SPARK_APP_NAME, id.name())
        .withLabel(SparkApplicationUtils.LABEL_SPARK_ROLE, SparkApplicationUtils.SPARK_ROLE_DRIVER)
        .list()
        .getItems()
        .stream()
        .filter(pod -> appUid == null || isOwnedBy(pod, appUid))
        .max(
            Comparator.comparing(
                pod -> pod.getMetadata().getCreationTimestamp(),
                Comparator.nullsFirst(Comparator.naturalOrder())));
  }

  private static boolean isOwnedBy(Pod pod, String uid) {
    List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
    return owners != null && owners.stream().anyMatch(owner -> uid.equals(owner.getUid()));
  }

  private static void setCancelRequested(GenericKubernetesResource app, boolean cancelRequested) {
    Map<String, String> annotations = new HashMap<>();
    if (app.getMetadata().getAnnotations() != null) {
      annotations.putAll(app.getMetadata().getAnnotations());
    }
    if (cancelRequested) {
      annotations.put(K8sJobResourceUtils.ANNOTATION_CANCEL_REQUESTED, "true");
    } else {
      annotations.remove(K8sJobResourceUtils.ANNOTATION_CANCEL_REQUESTED);
    }
    app.getMetadata().setAnnotations(annotations);
  }

  private void deleteSparkApplication(K8sJobExecutionId id) {
    // The SparkApplication stays until its pods are deleted, so that a status query sees it being
    // deleted rather than gone.
    getSparkApplicationClient(id.namespace())
        .withName(id.name())
        .withPropagationPolicy(DeletionPropagation.FOREGROUND)
        .delete();
    sparkApplicationCache.invalidate(id.namespace());
  }

  private NonNamespaceOperation<
          GenericKubernetesResource,
          GenericKubernetesResourceList,
          Resource<GenericKubernetesResource>>
      getSparkApplicationClient(String namespace) {
    return client
        .genericKubernetesResources(SparkApplicationUtils.SPARK_APPLICATION)
        .inNamespace(namespace);
  }
}
