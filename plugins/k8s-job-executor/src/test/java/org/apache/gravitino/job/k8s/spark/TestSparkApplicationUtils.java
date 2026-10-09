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
                    "spark.submit.deployMode", "client",
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
    for (String uri : new String[] {"/opt/app.jar", "app.jar", "file:///opt/app.jar"}) {
      SparkJobTemplate executable = newTemplate(uri, ImmutableMap.of());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, executable, configs));

      SparkJobTemplate jar =
          SparkJobTemplate.builder()
              .withName("user-job")
              .withExecutable("https://repo/app.jar")
              .withClassName("org.example.App")
              .withJars(ImmutableList.of(uri))
              .build();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, jar, configs));
    }
  }

  @Test
  public void testRejectEnvironments() {
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("user-job")
            .withExecutable("https://repo/app.jar")
            .withClassName("org.example.App")
            .withEnvironments(ImmutableMap.of("TOKEN", "secret"))
            .build();
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SparkApplicationUtils.buildSparkApplication(CONTEXT, template, configs));
    Assertions.assertTrue(e.getMessage().contains("TOKEN"));
    Assertions.assertFalse(e.getMessage().contains("secret"));
  }

  public static K8sJobExecutorConfigs newConfigs(Map<String, String> extra) {
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put("spark.conf.spark.driver.memory", "2g");
    map.putAll(extra);
    return new K8sJobExecutorConfigs(map);
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
