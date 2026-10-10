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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestK8sJobExecutorConfigs {

  public static Map<String, String> requiredConfigs() {
    return ImmutableMap.of(
        K8sJobExecutorConfigs.SPARK_IMAGE, "gravitino-spark:3.5.9",
        K8sJobExecutorConfigs.SPARK_VERSION, "3.5.9");
  }

  @Test
  public void testDefaults() {
    K8sJobExecutorConfigs configs = new K8sJobExecutorConfigs(requiredConfigs());

    Assertions.assertEquals("default", configs.cluster());
    Assertions.assertNull(configs.kubeconfig());
    Assertions.assertNull(configs.context());
    Assertions.assertNull(configs.masterUrl());
    Assertions.assertEquals("default", configs.namespace());
    Assertions.assertEquals("gravitino-job", configs.namePrefix());
    Assertions.assertEquals(5_000L, configs.statusCacheTtlMs());
    Assertions.assertEquals(600_000L, configs.noStatusTimeoutMs());
    Assertions.assertEquals("gravitino-spark:3.5.9", configs.sparkImage());
    Assertions.assertEquals("3.5.9", configs.sparkVersion());
    Assertions.assertEquals("spark", configs.sparkServiceAccount());
    Assertions.assertEquals(86_400_000L, configs.sparkResourceRetainDurationMs());
    Assertions.assertEquals(604_800_000L, configs.sparkTtlAfterStopMs());
    Assertions.assertEquals(3_600_000L, configs.sparkDriverStartTimeoutMs());
    Assertions.assertEquals(3_600_000L, configs.sparkDriverReadyTimeoutMs());
    Assertions.assertTrue(configs.sparkConf().isEmpty());
  }

  @Test
  public void testCustomValues() {
    Map<String, String> map = new HashMap<>(requiredConfigs());
    map.put(K8sJobExecutorConfigs.CLUSTER, "prod");
    map.put(K8sJobExecutorConfigs.NAMESPACE, " jobs ");
    map.put(K8sJobExecutorConfigs.NAME_PREFIX, "gvt-a");
    map.put(K8sJobExecutorConfigs.STATUS_CACHE_TTL_MS, "0");
    map.put(K8sJobExecutorConfigs.SPARK_TTL_AFTER_STOP_MS, "1000");
    map.put("spark.conf.spark.executor.instances", "2");
    map.put("spark.conf.", "ignored");

    K8sJobExecutorConfigs configs = new K8sJobExecutorConfigs(map);

    Assertions.assertEquals("prod", configs.cluster());
    Assertions.assertEquals("jobs", configs.namespace());
    Assertions.assertEquals("gvt-a", configs.namePrefix());
    Assertions.assertEquals(0L, configs.statusCacheTtlMs());
    Assertions.assertEquals(1000L, configs.sparkTtlAfterStopMs());
    Assertions.assertEquals(ImmutableMap.of("spark.executor.instances", "2"), configs.sparkConf());
  }

  @Test
  public void testRequiredSparkConfigs() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new K8sJobExecutorConfigs(
                ImmutableMap.of(K8sJobExecutorConfigs.SPARK_VERSION, "3.5.9")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new K8sJobExecutorConfigs(
                ImmutableMap.of(K8sJobExecutorConfigs.SPARK_IMAGE, "gravitino-spark:3.5.9")));
  }

  @Test
  public void testConnectionConfigs() {
    assertInvalid(
        K8sJobExecutorConfigs.MASTER_URL,
        "https://k8s:6443",
        K8sJobExecutorConfigs.KUBECONFIG,
        "/kube/config");
    assertInvalid(
        K8sJobExecutorConfigs.MASTER_URL,
        "https://k8s:6443",
        K8sJobExecutorConfigs.CONTEXT,
        "prod");
    assertInvalid(K8sJobExecutorConfigs.TOKEN_FILE, "/token");
    assertInvalid(K8sJobExecutorConfigs.CA_CERT_FILE, "/ca.crt");

    Map<String, String> map = new HashMap<>(requiredConfigs());
    map.put(K8sJobExecutorConfigs.MASTER_URL, "https://k8s:6443");
    map.put(K8sJobExecutorConfigs.CA_CERT_FILE, "/ca.crt");
    map.put(K8sJobExecutorConfigs.TOKEN_FILE, "/token");
    K8sJobExecutorConfigs configs = new K8sJobExecutorConfigs(map);
    Assertions.assertEquals("https://k8s:6443", configs.masterUrl());
    Assertions.assertEquals("/ca.crt", configs.caCertFile());
    Assertions.assertEquals("/token", configs.tokenFile());
  }

  @Test
  public void testInvalidValues() {
    assertInvalid(K8sJobExecutorConfigs.CLUSTER, "a/b");
    assertInvalid(K8sJobExecutorConfigs.NAMESPACE, "Gravitino_Jobs");
    assertInvalid(K8sJobExecutorConfigs.NAME_PREFIX, "Gravitino");
    assertInvalid(K8sJobExecutorConfigs.NAME_PREFIX, "gravitino-");
    assertInvalid(K8sJobExecutorConfigs.NAME_PREFIX, "a-name-prefix-that-is-too-long");
    assertInvalid(K8sJobExecutorConfigs.NO_STATUS_TIMEOUT_MS, "0");
    assertInvalid(K8sJobExecutorConfigs.SPARK_DRIVER_START_TIMEOUT_MS, "1h");
  }

  private static void assertInvalid(String... keyValues) {
    Map<String, String> map = new HashMap<>(requiredConfigs());
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    Assertions.assertThrows(IllegalArgumentException.class, () -> new K8sJobExecutorConfigs(map));
  }
}
