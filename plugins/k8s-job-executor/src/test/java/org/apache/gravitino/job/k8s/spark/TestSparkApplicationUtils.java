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
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.connector.job.JobContext;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.job.k8s.K8sJobExecutorConfigs;
import org.apache.gravitino.job.k8s.K8sJobResourceUtils;
import org.apache.gravitino.job.k8s.TestK8sJobExecutorConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSparkApplicationUtils {

  private static final JobContext CONTEXT = new JobContext(42L, "ml", new File("/tmp/job-42"));

  private final K8sJobExecutorConfigs configs = newConfigs(ImmutableMap.of());

  @Test
  public void testRejectClientDeployMode() {
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("user-job")
            .withExecutable("s3a://bucket/app.jar")
            .withClassName("org.example.App")
            .withConfigs(ImmutableMap.of("spark.submit.deployMode", " Client "))
            .build();
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs));
    Assertions.assertTrue(e.getMessage().contains("Client"), e.getMessage());

    // Nor set by the executor configurations.
    K8sJobExecutorConfigs clientMode =
        newConfigs(ImmutableMap.of("spark.conf.spark.submit.deployMode", "client"));
    SparkJobTemplate noMode =
        SparkJobTemplate.builder()
            .withName("user-job")
            .withExecutable("s3a://bucket/app.jar")
            .withClassName("org.example.App")
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, noMode, clientMode));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildSparkApplication() {
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("user-job")
            .withExecutable("s3a://bucket/app.jar")
            .withClassName("org.example.App")
            .withArguments(ImmutableList.of("--date", "2026-09-30"))
            .withJars(ImmutableList.of("https://repo/lib.jar"))
            .withFiles(ImmutableList.of("local:///opt/conf.properties"))
            .withArchives(ImmutableList.of("hdfs://nn/env.tgz#env"))
            .withConfigs(
                ImmutableMap.of(
                    "spark.master", "local[*]",
                    "spark.submit.deployMode", "cluster",
                    "spark.executor.instances", "2",
                    "spark.jars", "https://repo/other.jar"))
            .build();

    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs);

    Assertions.assertEquals("spark.apache.org/v1", app.getApiVersion());
    Assertions.assertEquals("SparkApplication", app.getKind());
    Assertions.assertEquals("gravitino-job-42", app.getMetadata().getName());
    Assertions.assertEquals("default", app.getMetadata().getNamespace());
    Assertions.assertEquals(
        ImmutableMap.of(
            "app.kubernetes.io/managed-by", "gravitino", "gravitino.apache.org/job-id", "42"),
        app.getMetadata().getLabels());
    Assertions.assertEquals(
        "ml", app.getMetadata().getAnnotations().get(K8sJobResourceUtils.ANNOTATION_METALAKE));

    Map<String, Object> spec = (Map<String, Object>) app.getAdditionalProperties().get("spec");
    Assertions.assertEquals("org.example.App", spec.get("mainClass"));
    Assertions.assertEquals("s3a://bucket/app.jar", spec.get("jars"));
    Assertions.assertFalse(spec.containsKey("pyFiles"));
    Assertions.assertEquals(ImmutableList.of("--date", "2026-09-30"), spec.get("driverArgs"));
    Assertions.assertEquals(ImmutableMap.of("sparkVersion", "3.5.9"), spec.get("runtimeVersions"));

    Map<String, String> sparkConf = (Map<String, String>) spec.get("sparkConf");
    Assertions.assertEquals(
        ImmutableMap.<String, String>builder()
            .put("spark.archives", "hdfs://nn/env.tgz#env")
            .put("spark.driver.memory", "2g")
            .put("spark.executor.instances", "2")
            .put("spark.files", "local:///opt/conf.properties")
            .put("spark.jars", "https://repo/other.jar,https://repo/lib.jar")
            .put("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
            .put("spark.kubernetes.container.image", "gravitino-spark:3.5.9")
            .put("spark.kubernetes.namespace", "default")
            .build(),
        sparkConf);

    Map<String, Object> tolerations = (Map<String, Object>) spec.get("applicationTolerations");
    Assertions.assertEquals(
        ImmutableMap.of("restartPolicy", "Never"), tolerations.get("restartConfig"));
    Assertions.assertEquals("Always", tolerations.get("resourceRetainPolicy"));
    Assertions.assertEquals(86_400_000L, tolerations.get("resourceRetainDurationMillis"));
    Assertions.assertEquals(604_800_000L, tolerations.get("ttlAfterStopMillis"));
    Assertions.assertEquals(
        ImmutableMap.of(
            "driverStartTimeoutMillis", 3_600_000L,
            "driverReadyTimeoutMillis", 3_600_000L),
        tolerations.get("applicationTimeoutConfig"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPythonApplication() {
    SparkJobTemplate template = newTemplate("https://repo/job.py", ImmutableMap.of());

    Map<String, Object> spec =
        (Map<String, Object>)
            SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs)
                .getAdditionalProperties()
                .get("spec");

    Assertions.assertEquals("https://repo/job.py", spec.get("pyFiles"));
    Assertions.assertFalse(spec.containsKey("jars"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPythonApplicationWithQuery() {
    for (String uri : new String[] {"https://repo/job.PY?sig=abc", "s3a://bucket/job.py#main"}) {
      Map<String, Object> spec =
          (Map<String, Object>)
              SparkApplicationUtils.buildSparkApplication(
                      CONTEXT, newTemplate(uri, ImmutableMap.of()), configs)
                  .getAdditionalProperties()
                  .get("spec");
      Assertions.assertEquals(uri, spec.get("pyFiles"));
    }

    Map<String, Object> jar =
        (Map<String, Object>)
            SparkApplicationUtils.buildSparkApplication(
                    CONTEXT,
                    newTemplate("https://repo/app.jar?name=job.py", ImmutableMap.of()),
                    configs)
                .getAdditionalProperties()
                .get("spec");
    Assertions.assertTrue(jar.containsKey("jars"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPythonApplicationWithoutMainClass() {
    for (String className : new String[] {null, " "}) {
      SparkJobTemplate template =
          SparkJobTemplate.builder()
              .withName("user-job")
              .withExecutable("https://repo/job.py")
              .withClassName(className)
              .build();
      Map<String, Object> spec =
          (Map<String, Object>)
              SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs)
                  .getAdditionalProperties()
                  .get("spec");
      Assertions.assertEquals("https://repo/job.py", spec.get("pyFiles"));
      Assertions.assertFalse(spec.containsKey("mainClass"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBlankAndExistingConfigs() {
    SparkJobTemplate template =
        newTemplateBuilder()
            .withFiles(ImmutableList.of("https://repo/b.conf"))
            .withArchives(ImmutableList.of("https://repo/b.tgz"))
            .withConfigs(
                ImmutableMap.of(
                    // Blank values don't override the executor configurations.
                    "spark.kubernetes.container.image", " ",
                    "spark.kubernetes.authenticate.driver.serviceAccountName", "",
                    "spark.kubernetes.namespace", " ",
                    "spark.files", "https://repo/a.conf",
                    "spark.archives", "https://repo/a.tgz"))
            .build();

    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs);

    Assertions.assertEquals("default", app.getMetadata().getNamespace());
    Map<String, String> sparkConf =
        (Map<String, String>)
            ((Map<String, Object>) app.getAdditionalProperties().get("spec")).get("sparkConf");
    Assertions.assertEquals(
        "gravitino-spark:3.5.9", sparkConf.get("spark.kubernetes.container.image"));
    Assertions.assertEquals(
        "spark", sparkConf.get("spark.kubernetes.authenticate.driver.serviceAccountName"));
    Assertions.assertEquals("default", sparkConf.get("spark.kubernetes.namespace"));
    Assertions.assertEquals(
        "https://repo/a.conf,https://repo/b.conf", sparkConf.get("spark.files"));
    Assertions.assertEquals(
        "https://repo/a.tgz,https://repo/b.tgz", sparkConf.get("spark.archives"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDefaultSparkConfigsDoNotChooseNamespace() {
    // Only the namespace configuration of the executor, or the job itself, chooses the namespace.
    K8sJobExecutorConfigs withDefault =
        newConfigs(
            ImmutableMap.of(
                K8sJobExecutorConfigs.NAMESPACE,
                "jobs",
                "spark.conf.spark.kubernetes.namespace",
                "other"));

    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(
            CONTEXT, newTemplate("https://repo/app.jar", ImmutableMap.of()), withDefault);

    Assertions.assertEquals("jobs", app.getMetadata().getNamespace());
    Map<String, String> sparkConf =
        (Map<String, String>)
            ((Map<String, Object>) app.getAdditionalProperties().get("spec")).get("sparkConf");
    Assertions.assertEquals("jobs", sparkConf.get("spark.kubernetes.namespace"));
  }

  @Test
  public void testNamespaceOfJob() {
    Assertions.assertEquals(
        "team-a",
        SparkApplicationUtils.buildSparkApplication(
                CONTEXT,
                newTemplate(
                    "https://repo/app.jar",
                    ImmutableMap.of("spark.kubernetes.namespace", " team-a ")),
                configs)
            .getMetadata()
            .getNamespace());

    for (String invalid : new String[] {"Team_A", "team/a", "-team"}) {
      SparkJobTemplate template =
          newTemplate(
              "https://repo/app.jar", ImmutableMap.of("spark.kubernetes.namespace", invalid));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testJobConfigsOverrideDefaults() {
    K8sJobExecutorConfigs withDefaults =
        newConfigs(
            ImmutableMap.of(
                K8sJobExecutorConfigs.NAMESPACE,
                "jobs",
                "spark.conf.spark.executor.instances",
                "1",
                "spark.conf.spark.master",
                "k8s://ignored"));
    SparkJobTemplate template =
        newTemplate(
            "https://repo/app.jar",
            ImmutableMap.of(
                "spark.executor.instances", "4",
                "spark.kubernetes.namespace", "team-a",
                "spark.kubernetes.container.image", "custom:1.0",
                "spark.kubernetes.authenticate.driver.serviceAccountName", "team-a-spark"));

    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(CONTEXT, template, withDefaults);

    Assertions.assertEquals("team-a", app.getMetadata().getNamespace());
    Map<String, String> sparkConf =
        (Map<String, String>)
            ((Map<String, Object>) app.getAdditionalProperties().get("spec")).get("sparkConf");
    Assertions.assertEquals("4", sparkConf.get("spark.executor.instances"));
    Assertions.assertEquals("custom:1.0", sparkConf.get("spark.kubernetes.container.image"));
    Assertions.assertEquals(
        "team-a-spark", sparkConf.get("spark.kubernetes.authenticate.driver.serviceAccountName"));
    Assertions.assertFalse(sparkConf.containsKey("spark.master"));
  }

  @Test
  public void testDefaultNamespace() {
    K8sJobExecutorConfigs withNamespace =
        newConfigs(ImmutableMap.of(K8sJobExecutorConfigs.NAMESPACE, "jobs"));
    GenericKubernetesResource app =
        SparkApplicationUtils.buildSparkApplication(
            CONTEXT, newTemplate("https://repo/app.jar", ImmutableMap.of()), withNamespace);
    Assertions.assertEquals("jobs", app.getMetadata().getNamespace());
  }

  @Test
  public void testRejectServerLocalResources() {
    String[] serverPaths = {
      "/opt/app.jar",
      "app.jar",
      "file:///opt/app.jar",
      "FILE:/opt/app.jar",
      "C:/apps/app.jar",
      // Spark takes several resources in one entry, each of them is checked.
      "https://repo/a.jar,/opt/app.jar",
    };
    for (String uri : serverPaths) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SparkApplicationUtils.buildSparkApplication(
                  CONTEXT, newTemplate(uri, ImmutableMap.of()), configs),
          uri);
      for (SparkJobTemplate template :
          ImmutableList.of(
              newTemplateBuilder().withJars(ImmutableList.of(uri)).build(),
              newTemplateBuilder().withFiles(ImmutableList.of(uri)).build(),
              newTemplateBuilder().withArchives(ImmutableList.of(uri)).build())) {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs),
            uri);
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAcceptClusterReachableResources() {
    // Whatever has a scheme other than file is left to Spark, even if it isn't a strict URI.
    String[] uris = {
      "local:///opt/app.jar",
      "hdfs:/apps/app.jar",
      "s3a://bucket/my app.jar",
      "https://repo/a.jar, https://repo/b.jar",
    };
    for (String uri : uris) {
      Map<String, Object> spec =
          (Map<String, Object>)
              SparkApplicationUtils.buildSparkApplication(
                      CONTEXT, newTemplate(uri, ImmutableMap.of()), configs)
                  .getAdditionalProperties()
                  .get("spec");
      Assertions.assertEquals(uri, spec.get("jars"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testResourcesInSparkConfigs() {
    String[] keys = {"spark.jars", "spark.files", "spark.archives"};
    String[] serverPaths = {
      "/opt/local.jar", "file:///opt/local.jar", "https://repo/a.jar,/opt/local.jar"
    };
    for (String key : keys) {
      for (String value : serverPaths) {
        // Set by the job.
        SparkJobTemplate template =
            newTemplate("https://repo/app.jar", ImmutableMap.of(key, value));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs),
            key + "=" + value);

        // Set by the default Spark configurations of the executor.
        K8sJobExecutorConfigs withDefault = newConfigs(ImmutableMap.of("spark.conf." + key, value));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkApplicationUtils.buildSparkApplication(
                    CONTEXT, newTemplate("https://repo/app.jar", ImmutableMap.of()), withDefault),
            key + "=" + value);
      }

      // Resources that the cluster can reach are kept, from both sources. The job's value
      // overrides the default one, and an empty element is ignored like Spark does.
      K8sJobExecutorConfigs withDefault =
          newConfigs(ImmutableMap.of("spark.conf." + key, "local:///opt/default.jar"));
      Map<String, String> defaults =
          (Map<String, String>)
              ((Map<String, Object>)
                      SparkApplicationUtils.buildSparkApplication(
                              CONTEXT,
                              newTemplate("https://repo/app.jar", ImmutableMap.of()),
                              withDefault)
                          .getAdditionalProperties()
                          .get("spec"))
                  .get("sparkConf");
      Assertions.assertEquals("local:///opt/default.jar", defaults.get(key));

      String value = "https://repo/a.jar,, s3a://bucket/b.jar";
      Map<String, String> overridden =
          (Map<String, String>)
              ((Map<String, Object>)
                      SparkApplicationUtils.buildSparkApplication(
                              CONTEXT,
                              newTemplate("https://repo/app.jar", ImmutableMap.of(key, value)),
                              withDefault)
                          .getAdditionalProperties()
                          .get("spec"))
                  .get("sparkConf");
      Assertions.assertEquals(value, overridden.get(key));
    }
  }

  @Test
  public void testServerPathInSparkConfigsDoesNotLeakCredentials() {
    SparkJobTemplate template =
        newTemplate(
            "https://repo/app.jar",
            ImmutableMap.of("spark.files", "https://repo/a.conf,file://u:secret@h/b.conf?sig=abc"));
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs));
    Assertions.assertTrue(e.getMessage().contains("file://h/b.conf "), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("secret"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("abc"), e.getMessage());
  }

  @Test
  public void testRejectionDoesNotLeakCredentials() {
    SparkJobTemplate template =
        newTemplate("file://user:secret@host/opt/app.jar?X-Signature=abc#frag", ImmutableMap.of());
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs));
    Assertions.assertTrue(e.getMessage().contains("file://host/opt/app.jar "), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("secret"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("abc"), e.getMessage());
  }

  @Test
  public void testBuiltinJobRunsBuiltinJobsJar() {
    // The executable of a built-in job template is the jobs jar on the Gravitino server.
    for (String uri :
        new String[] {
          "/gravitino/auxlib/gravitino-jobs-2.0.0.jar", "file:///gravitino/auxlib/jobs.jar"
        }) {
      SparkJobTemplate template =
          newTemplateBuilder().withName("builtin-sparkpi").withExecutable(uri).build();
      Assertions.assertEquals(
          "local:///opt/gravitino/jobs/gravitino-jobs.jar", getSpec(template, configs).get("jars"));
      Assertions.assertEquals(
          "s3a://bucket/gravitino-jobs.jar",
          getSpec(
                  template,
                  newConfigs(
                      ImmutableMap.of(
                          K8sJobExecutorConfigs.SPARK_BUILTIN_JOBS_JAR,
                          "s3a://bucket/gravitino-jobs.jar")))
              .get("jars"));
    }
  }

  @Test
  public void testBuiltinJobKeepsClusterReachableExecutable() {
    SparkJobTemplate template =
        newTemplateBuilder()
            .withName("builtin-sparkpi")
            .withExecutable("https://repo/gravitino-jobs.jar")
            .build();
    Assertions.assertEquals(
        "https://repo/gravitino-jobs.jar", getSpec(template, configs).get("jars"));
  }

  @Test
  public void testBuiltinJobsJarOnlyReplacesExecutableOfBuiltinJobs() {
    // Not a built-in job: "builtin-" must be the prefix of the name.
    SparkJobTemplate userJob =
        newTemplateBuilder().withName("my-builtin-job").withExecutable("/opt/app.jar").build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, userJob, configs));

    // The other resources of a built-in job are still checked.
    SparkJobTemplate builtinJob =
        newTemplateBuilder()
            .withName("builtin-sparkpi")
            .withExecutable("/gravitino/auxlib/gravitino-jobs-2.0.0.jar")
            .withJars(ImmutableList.of("/gravitino/auxlib/other.jar"))
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, builtinJob, configs));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEnvironments() {
    K8sJobExecutorConfigs withDefaults =
        newConfigs(
            ImmutableMap.of(
                "spark.conf.spark.kubernetes.driverEnv.REGION",
                "default-region",
                "spark.conf.spark.executorEnv.STAGE",
                "default-stage"));
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("user-job")
            .withExecutable("https://repo/app.jar")
            .withClassName("org.example.App")
            .withEnvironments(ImmutableMap.of("REGION", "us-west", "app.mode-1", "", "TZ", "UTC"))
            .withConfigs(ImmutableMap.of("spark.executorEnv.TZ", "Asia/Shanghai"))
            .build();

    Map<String, String> sparkConf =
        (Map<String, String>) getSpec(template, withDefaults).get("sparkConf");

    // Set for the driver and the executors, in place of the defaults of the job executor.
    Assertions.assertEquals("us-west", sparkConf.get("spark.kubernetes.driverEnv.REGION"));
    Assertions.assertEquals("us-west", sparkConf.get("spark.executorEnv.REGION"));
    Assertions.assertEquals("", sparkConf.get("spark.kubernetes.driverEnv.app.mode-1"));
    Assertions.assertEquals("", sparkConf.get("spark.executorEnv.app.mode-1"));
    // The Spark configurations of the job template take precedence.
    Assertions.assertEquals("UTC", sparkConf.get("spark.kubernetes.driverEnv.TZ"));
    Assertions.assertEquals("Asia/Shanghai", sparkConf.get("spark.executorEnv.TZ"));
    // A default that the job template doesn't set is kept.
    Assertions.assertEquals("default-stage", sparkConf.get("spark.executorEnv.STAGE"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAcceptEnvironmentNames() {
    for (String valid : new String[] {"-X", ".X", "_X", "a.b-c_1", ".a.."}) {
      SparkJobTemplate template =
          SparkJobTemplate.builder()
              .withName("user-job")
              .withExecutable("https://repo/app.jar")
              .withClassName("org.example.App")
              .withEnvironments(ImmutableMap.of(valid, "v"))
              .build();
      Map<String, String> sparkConf =
          (Map<String, String>) getSpec(template, configs).get("sparkConf");
      Assertions.assertEquals("v", sparkConf.get("spark.kubernetes.driverEnv." + valid), valid);
      Assertions.assertEquals("v", sparkConf.get("spark.executorEnv." + valid), valid);
    }
  }

  @Test
  public void testRejectInvalidEnvironmentName() {
    for (String invalid : new String[] {"", "MY VAR", "A=B", "1ST", "PATH/X", ".", "..", "..CFG"}) {
      SparkJobTemplate template =
          SparkJobTemplate.builder()
              .withName("user-job")
              .withExecutable("https://repo/app.jar")
              .withClassName("org.example.App")
              .withEnvironments(ImmutableMap.of(invalid, "secret-value"))
              .build();
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs));
      Assertions.assertTrue(e.getMessage().contains("user-job"), e.getMessage());
      Assertions.assertTrue(e.getMessage().contains("'" + invalid + "'"), e.getMessage());
      // The value of an environment variable never goes into an error message.
      Assertions.assertFalse(e.getMessage().contains("secret-value"), e.getMessage());
    }
  }

  public static K8sJobExecutorConfigs newConfigs(Map<String, String> extra) {
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put("spark.conf.spark.driver.memory", "2g");
    map.putAll(extra);
    return new K8sJobExecutorConfigs(map);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> getSpec(
      SparkJobTemplate template, K8sJobExecutorConfigs configs) {
    return (Map<String, Object>)
        SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs)
            .getAdditionalProperties()
            .get("spec");
  }

  private static SparkJobTemplate.Builder newTemplateBuilder() {
    return SparkJobTemplate.builder()
        .withName("user-job")
        .withExecutable("https://repo/app.jar")
        .withClassName("org.example.App");
  }

  private static SparkJobTemplate newTemplate(String executable, Map<String, String> configs) {
    return SparkJobTemplate.builder()
        .withName("user-job")
        .withExecutable(executable)
        .withClassName("org.example.App")
        .withConfigs(configs)
        .build();
  }
}
