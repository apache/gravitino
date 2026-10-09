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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.server.mock.KubernetesMixedDispatcher;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.mockwebserver.Context;
import io.fabric8.mockwebserver.MockWebServer;
import io.fabric8.mockwebserver.ServerRequest;
import io.fabric8.mockwebserver.ServerResponse;
import io.fabric8.mockwebserver.http.RecordedRequest;
import java.io.File;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.connector.job.JobContext;
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.job.k8s.spark.SparkApplicationUtils;
import org.apache.gravitino.job.k8s.spark.TestSparkApplicationUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestK8sJobExecutor {

  private static final String APPS_PATH =
      "/apis/spark.apache.org/v1/namespaces/default/sparkapplications";

  private KubernetesMockServer server;

  private KubernetesClient client;

  private K8sJobExecutor executor;

  private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

  @BeforeEach
  public void setUp() {
    Map<ServerRequest, Queue<ServerResponse>> responses = new HashMap<>();
    server =
        new KubernetesMockServer(
            new Context(),
            new MockWebServer(),
            responses,
            new KubernetesMixedDispatcher(responses),
            false);
    server.init();
    server.expectCustomResource(
        new CustomResourceDefinitionContext.Builder()
            .withGroup(SparkApplicationUtils.SPARK_APPLICATION.getGroup())
            .withVersion(SparkApplicationUtils.SPARK_APPLICATION.getVersion())
            .withKind(SparkApplicationUtils.SPARK_APPLICATION.getKind())
            .withPlural(SparkApplicationUtils.SPARK_APPLICATION.getPlural())
            .withScope("Namespaced")
            .build());
    client = server.createClient();

    executor =
        new K8sJobExecutor(
            new Clock() {
              @Override
              public ZoneId getZone() {
                return ZoneOffset.UTC;
              }

              @Override
              public Clock withZone(ZoneId zone) {
                return this;
              }

              @Override
              public Instant instant() {
                return now.get();
              }
            });
    executor.initialize(
        TestSparkApplicationUtils.newConfigs(
            ImmutableMap.of(K8sJobExecutorConfigs.STATUS_CACHE_TTL_MS, "0")),
        client);
  }

  @AfterEach
  public void tearDown() {
    executor.close();
    server.destroy();
  }

  @Test
  public void testSubmitAndQuery() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());

    Assertions.assertEquals("default/sparkapp/default/gravitino-job-1", jobExecutionId);
    GenericKubernetesResource app = sparkApplication("gravitino-job-1");
    Assertions.assertNotNull(app);
    Assertions.assertEquals(
        "1", app.getMetadata().getLabels().get(K8sJobResourceUtils.LABEL_JOB_ID));
    Assertions.assertEquals(
        JobHandle.Status.QUEUED, executor.getJobExecutionInfo(jobExecutionId).status());

    setStates("gravitino-job-1", "Submitted", "DriverReady", "RunningHealthy");
    JobExecutionInfo running = executor.getJobExecutionInfo(jobExecutionId);
    Assertions.assertEquals(JobHandle.Status.STARTED, running.status());
    Assertions.assertNotNull(running.startedAt());

    setStates("gravitino-job-1", "Submitted", "DriverReady", "Succeeded", "ResourceReleased");
    Assertions.assertEquals(
        JobHandle.Status.SUCCEEDED, executor.getJobExecutionInfo(jobExecutionId).status());

    // The times of a finished job come from its driver pod.
    client
        .pods()
        .inNamespace("default")
        .resource(
            new PodBuilder()
                .withNewMetadata()
                .withName("driver")
                .addToLabels(SparkApplicationUtils.LABEL_SPARK_APP_NAME, "gravitino-job-1")
                .addToLabels(SparkApplicationUtils.LABEL_SPARK_ROLE, "driver")
                .endMetadata()
                .withNewStatus()
                .addNewContainerStatus()
                .withName("spark-kubernetes-driver")
                .withNewState()
                .withNewTerminated()
                .withStartedAt("2026-09-29T00:00:00Z")
                .withFinishedAt("2026-09-29T00:00:01Z")
                .endTerminated()
                .endState()
                .endContainerStatus()
                .endStatus()
                .build())
        .create();
    JobExecutionInfo succeeded = executor.getJobExecutionInfo(jobExecutionId);
    Assertions.assertEquals(Instant.parse("2026-09-29T00:00:00Z"), succeeded.startedAt());
    Assertions.assertEquals(Instant.parse("2026-09-29T00:00:01Z"), succeeded.finishedAt());
  }

  @Test
  public void testStatusCacheFallsBackToGet() {
    executor.initialize(
        TestSparkApplicationUtils.newConfigs(
            ImmutableMap.of(K8sJobExecutorConfigs.STATUS_CACHE_TTL_MS, "60000")),
        client);
    String job1 = executor.submitJob(context(1L), sparkTemplate());
    Assertions.assertEquals(JobHandle.Status.QUEUED, executor.getJobExecutionInfo(job1).status());

    // Created after the namespace was listed, so it isn't in the cached listing.
    String job2 = executor.submitJob(context(2L), sparkTemplate());
    Assertions.assertEquals(JobHandle.Status.QUEUED, executor.getJobExecutionInfo(job2).status());

    // The cached listing is used within its TTL, so the new state isn't seen yet.
    setStates("gravitino-job-1", "Submitted", "DriverReady");
    Assertions.assertEquals(JobHandle.Status.QUEUED, executor.getJobExecutionInfo(job1).status());
  }

  @Test
  public void testNamespaceOfJob() {
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("user-job")
            .withExecutable("local:///opt/app.jar")
            .withClassName("org.example.App")
            .withConfigs(ImmutableMap.of(SparkApplicationUtils.SPARK_NAMESPACE, "team-a"))
            .build();

    String jobExecutionId = executor.submitJob(context(1L), template);

    Assertions.assertEquals("default/sparkapp/team-a/gravitino-job-1", jobExecutionId);
    Assertions.assertNull(sparkApplication("gravitino-job-1"));
    Assertions.assertEquals(
        JobHandle.Status.QUEUED, executor.getJobExecutionInfo(jobExecutionId).status());
    executor.cancelJob(jobExecutionId);
    Assertions.assertThrows(
        NoSuchJobException.class, () -> executor.getJobExecutionInfo(jobExecutionId));
  }

  @Test
  public void testRejectedByKubernetes() {
    server
        .expect()
        .post()
        .withPath(APPS_PATH)
        .andReturn(404, new StatusBuilder().withCode(404).withMessage("no such namespace").build())
        .once();

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> executor.submitJob(context(1L), sparkTemplate()));
    Assertions.assertTrue(e.getMessage().contains("no such namespace"), e.getMessage());

    // Other failures aren't the job's fault. The client retries server errors, so use another.
    server
        .expect()
        .post()
        .withPath(APPS_PATH)
        .andReturn(410, new StatusBuilder().withCode(410).build())
        .once();
    Assertions.assertThrows(
        KubernetesClientException.class, () -> executor.submitJob(context(1L), sparkTemplate()));
  }

  @Test
  public void testFailJobWithoutStatus() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    now.set(now.get().plusSeconds(601));

    // The SparkApplication can't be deleted, so the job isn't failed yet.
    server
        .expect()
        .delete()
        .withPath(APPS_PATH + "/gravitino-job-1")
        .andReturn(403, new StatusBuilder().withCode(403).build())
        .once();
    Assertions.assertThrows(
        KubernetesClientException.class, () -> executor.getJobExecutionInfo(jobExecutionId));
    Assertions.assertNotNull(sparkApplication("gravitino-job-1"));

    // Deleted, so that the operator doesn't run it after the job failed.
    Assertions.assertEquals(
        JobHandle.Status.FAILED, executor.getJobExecutionInfo(jobExecutionId).status());
    Assertions.assertNull(sparkApplication("gravitino-job-1"));
  }

  @Test
  public void testDeleteAliveDriverOfFailedJob() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    setStates(
        "gravitino-job-1",
        "Submitted",
        "DriverRequested",
        "DriverStartTimedOut",
        "TerminatedWithoutReleaseResources");
    // The pending driver pod is retained, and would run once it can be scheduled.
    createDriverPod("driver", "spark-kubernetes-driver");

    Assertions.assertEquals(
        JobHandle.Status.FAILED, executor.getJobExecutionInfo(jobExecutionId).status());
    Assertions.assertNull(client.pods().inNamespace("default").withName("driver").get());
    Assertions.assertNotNull(sparkApplication("gravitino-job-1"));
  }

  @Test
  public void testKeepTerminatedDriverOfFailedJob() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    setStates("gravitino-job-1", "Submitted", "DriverReady", "Failed");
    client
        .pods()
        .inNamespace("default")
        .resource(
            new PodBuilder()
                .withNewMetadata()
                .withName("driver")
                .addToLabels(SparkApplicationUtils.LABEL_SPARK_APP_NAME, "gravitino-job-1")
                .addToLabels(SparkApplicationUtils.LABEL_SPARK_ROLE, "driver")
                .endMetadata()
                .withNewStatus()
                .withPhase("Failed")
                .endStatus()
                .build())
        .create();

    Assertions.assertEquals(
        JobHandle.Status.FAILED, executor.getJobExecutionInfo(jobExecutionId).status());
    // Kept, so that its output can still be read.
    Assertions.assertNotNull(client.pods().inNamespace("default").withName("driver").get());
  }

  @Test
  public void testListFailureFallsBackToGet() throws InterruptedException {
    executor.initialize(
        TestSparkApplicationUtils.newConfigs(
            ImmutableMap.of(K8sJobExecutorConfigs.STATUS_CACHE_TTL_MS, "60000")),
        client);
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    server
        .expect()
        .get()
        .withPath(APPS_PATH + "?labelSelector=app.kubernetes.io%2Fmanaged-by%3Dgravitino")
        .andReturn(403, new StatusBuilder().withCode(403).build())
        .always();

    Assertions.assertEquals(
        JobHandle.Status.QUEUED, executor.getJobExecutionInfo(jobExecutionId).status());
    // Each query gets the SparkApplication itself, so it sees the new state right away.
    setStates("gravitino-job-1", "Submitted", "DriverReady");
    Assertions.assertEquals(
        JobHandle.Status.STARTED, executor.getJobExecutionInfo(jobExecutionId).status());

    // The namespace is listed once, then the failure is cached.
    int lists = 0;
    RecordedRequest request;
    while ((request = server.takeRequest(10, TimeUnit.MILLISECONDS)) != null) {
      if ("GET".equals(request.getMethod()) && request.getPath().contains("labelSelector")) {
        lists++;
      }
    }
    Assertions.assertEquals(1, lists);
  }

  @Test
  public void testResubmitSameJob() {
    String first = executor.submitJob(context(1L), sparkTemplate());
    String second = executor.submitJob(context(1L), sparkTemplate());
    Assertions.assertEquals(first, second);
  }

  @Test
  public void testRejectResourceOfAnotherJob() {
    GenericKubernetesResource other =
        SparkApplicationUtils.buildSparkApplication(
            context(2L), sparkTemplate(), TestSparkApplicationUtils.newConfigs(ImmutableMap.of()));
    other.getMetadata().setName("gravitino-job-1");
    client
        .genericKubernetesResources(SparkApplicationUtils.SPARK_APPLICATION)
        .inNamespace("default")
        .resource(other)
        .create();

    Assertions.assertThrows(
        IllegalStateException.class, () -> executor.submitJob(context(1L), sparkTemplate()));
  }

  @Test
  public void testRejectShellJob() {
    ShellJobTemplate template =
        ShellJobTemplate.builder().withName("shell-job").withExecutable("echo").build();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> executor.submitJob(context(1L), template));
  }

  @Test
  public void testNoSuchJob() {
    Assertions.assertThrows(
        NoSuchJobException.class,
        () -> executor.getJobExecutionInfo("default/sparkapp/default/gravitino-job-9"));
    Assertions.assertThrows(
        NoSuchJobException.class,
        () -> executor.cancelJob("default/sparkapp/default/gravitino-job-9"));
    Assertions.assertThrows(
        NoSuchJobException.class, () -> executor.getJobExecutionInfo("gravitino-job-9"));

    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    Assertions.assertThrows(
        NoSuchJobException.class,
        () -> executor.getJobExecutionInfo(jobExecutionId.replaceFirst("^default/", "other/")));
    Assertions.assertThrows(
        NoSuchJobException.class,
        () -> executor.getJobExecutionInfo(jobExecutionId.replace("/sparkapp/", "/job/")));
  }

  @Test
  public void testCancel() throws InterruptedException {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    setStates("gravitino-job-1", "Submitted", "DriverReady");
    // Keep the SparkApplication, as it stays until its pods are deleted.
    server
        .expect()
        .delete()
        .withPath(APPS_PATH + "/gravitino-job-1")
        .andReturn(200, sparkApplication("gravitino-job-1"))
        .always();

    executor.cancelJob(jobExecutionId);

    GenericKubernetesResource app = sparkApplication("gravitino-job-1");
    Assertions.assertTrue(K8sJobResourceUtils.isCancelRequested(app));
    RecordedRequest delete = lastRequest("DELETE");
    Assertions.assertTrue(delete.getBody().readUtf8().contains("\"Foreground\""));

    // The job is only cancelled once the deletion takes effect, and a status query never deletes.
    Assertions.assertEquals(
        JobHandle.Status.STARTED, executor.getJobExecutionInfo(jobExecutionId).status());
    Assertions.assertNull(lastRequest("DELETE"));
  }

  @Test
  public void testCancelAgainAfterFailedDeletion() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    setStates("gravitino-job-1", "Submitted", "DriverReady");
    server
        .expect()
        .delete()
        .withPath(APPS_PATH + "/gravitino-job-1")
        .andReturn(403, new StatusBuilder().withCode(403).build())
        .once();

    // The cancellation fails, and the job keeps running.
    Assertions.assertThrows(
        KubernetesClientException.class, () -> executor.cancelJob(jobExecutionId));
    Assertions.assertEquals(
        JobHandle.Status.STARTED, executor.getJobExecutionInfo(jobExecutionId).status());

    // Cancelling it again deletes it.
    executor.cancelJob(jobExecutionId);
    Assertions.assertNull(sparkApplication("gravitino-job-1"));
    Assertions.assertThrows(
        NoSuchJobException.class, () -> executor.getJobExecutionInfo(jobExecutionId));
  }

  @Test
  public void testCancelStoppedJob() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    setStates("gravitino-job-1", "Submitted", "DriverReady", "Succeeded");

    executor.cancelJob(jobExecutionId);

    GenericKubernetesResource app = sparkApplication("gravitino-job-1");
    Assertions.assertFalse(K8sJobResourceUtils.isCancelRequested(app));
    Assertions.assertEquals(
        JobHandle.Status.SUCCEEDED, executor.getJobExecutionInfo(jobExecutionId).status());
  }

  @Test
  public void testStdout() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    Assertions.assertEquals(ImmutableList.of(), executor.getJobStdout(jobExecutionId, 10, 1024));

    for (String name : new String[] {"driver", "executor"}) {
      client
          .pods()
          .inNamespace("default")
          .resource(
              new PodBuilder()
                  .withNewMetadata()
                  .withName(name)
                  .addToLabels(SparkApplicationUtils.LABEL_SPARK_APP_NAME, "gravitino-job-1")
                  .addToLabels(
                      SparkApplicationUtils.LABEL_SPARK_ROLE,
                      name.equals("executor") ? "executor" : "driver")
                  .endMetadata()
                  .build())
          .create();
    }
    server
        .expect()
        .get()
        .withPath("/api/v1/namespaces/default/pods/driver/log?pretty=false&tailLines=10")
        .andReturn(200, "line 1\nline 2\n")
        .always();

    Assertions.assertEquals(
        ImmutableList.of("line 1", "line 2"), executor.getJobStdout(jobExecutionId, 10, 1024));
    Assertions.assertEquals(ImmutableList.of(), executor.getJobStderr(jobExecutionId, 10, 1024));
    Assertions.assertEquals(ImmutableList.of(), executor.getJobStdout("invalid", 10, 1024));
  }

  @Test
  public void testStdoutFailure() {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    createDriverPod("driver", "spark-kubernetes-driver");
    server
        .expect()
        .get()
        .withPath("/api/v1/namespaces/default/pods/driver/log?pretty=false&tailLines=10")
        .andReturn(403, "forbidden")
        .always();

    Assertions.assertEquals(ImmutableList.of(), executor.getJobStdout(jobExecutionId, 10, 1024));
  }

  @Test
  public void testStdoutOfDriverWithSidecar() throws InterruptedException {
    String jobExecutionId = executor.submitJob(context(1L), sparkTemplate());
    createDriverPod("driver", "spark-kubernetes-driver", "sidecar");

    executor.getJobStdout(jobExecutionId, 10, 1024);

    RecordedRequest log = null;
    RecordedRequest request;
    while ((request = server.takeRequest(10, TimeUnit.MILLISECONDS)) != null) {
      if (request.getPath().contains("/pods/driver/log")) {
        log = request;
      }
    }
    Assertions.assertNotNull(log);
    Assertions.assertTrue(
        log.getPath().contains("container=spark-kubernetes-driver"), log.getPath());
  }

  private void createDriverPod(String name, String... containers) {
    PodBuilder pod =
        new PodBuilder()
            .withNewMetadata()
            .withName(name)
            .addToLabels(SparkApplicationUtils.LABEL_SPARK_APP_NAME, "gravitino-job-1")
            .addToLabels(SparkApplicationUtils.LABEL_SPARK_ROLE, "driver")
            .endMetadata()
            .withNewSpec()
            .endSpec();
    for (String container : containers) {
      pod.editSpec().addNewContainer().withName(container).endContainer().endSpec();
    }
    client.pods().inNamespace("default").resource(pod.build()).create();
  }

  private GenericKubernetesResource sparkApplication(String name) {
    return client
        .genericKubernetesResources(SparkApplicationUtils.SPARK_APPLICATION)
        .inNamespace("default")
        .withName(name)
        .get();
  }

  private void setStates(String name, String... states) {
    Map<String, Object> history = new HashMap<>();
    Instant time = Instant.parse("2026-09-30T00:00:00Z");
    for (int i = 0; i < states.length; i++) {
      history.put(
          String.valueOf(i),
          ImmutableMap.of(
              "currentStateSummary",
              states[i],
              "lastTransitionTime",
              time.plusSeconds(i).toString()));
    }
    GenericKubernetesResource app = sparkApplication(name);
    app.setAdditionalProperty(
        "status",
        ImmutableMap.of(
            "currentState",
            history.get(String.valueOf(states.length - 1)),
            "stateTransitionHistory",
            history));
    client
        .genericKubernetesResources(SparkApplicationUtils.SPARK_APPLICATION)
        .inNamespace("default")
        .resource(app)
        .update();
  }

  private RecordedRequest lastRequest(String method) throws InterruptedException {
    RecordedRequest last = null;
    RecordedRequest request;
    while ((request = server.takeRequest(10, TimeUnit.MILLISECONDS)) != null) {
      if (method.equals(request.getMethod())) {
        last = request;
      }
    }
    return last;
  }

  private static JobContext context(long jobId) {
    return new JobContext(jobId, "ml", new File("/tmp/job-" + jobId));
  }

  private static SparkJobTemplate sparkTemplate() {
    return SparkJobTemplate.builder()
        .withName("user-job")
        .withExecutable("local:///opt/spark/examples/jars/spark-examples.jar")
        .withClassName("org.apache.spark.examples.SparkPi")
        .withArguments(ImmutableList.of("10"))
        .build();
  }
}
