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
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestK8sClientUtils {

  private static final String KUBECONFIG =
      "apiVersion: v1\n"
          + "kind: Config\n"
          + "current-context: dev\n"
          + "clusters:\n"
          + "- name: dev\n"
          + "  cluster: {server: 'https://dev:6443'}\n"
          + "- name: prod\n"
          + "  cluster: {server: 'https://prod:6443'}\n"
          + "users:\n"
          + "- name: admin\n"
          + "  user: {token: admin-token}\n"
          + "contexts:\n"
          + "- name: dev\n"
          + "  context: {cluster: dev, user: admin}\n"
          + "- name: prod\n"
          + "  context: {cluster: prod, user: admin}\n";

  @TempDir Path dir;

  @Test
  public void testMasterUrl() throws IOException {
    Path caCert = Files.write(dir.resolve("ca.crt"), "cert".getBytes(StandardCharsets.UTF_8));
    Path token = Files.write(dir.resolve("token"), "token-1\n".getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.MASTER_URL, "https://jobs:6443");
    map.put(K8sJobExecutorConfigs.CA_CERT_FILE, caCert.toString());
    map.put(K8sJobExecutorConfigs.TOKEN_FILE, token.toString());

    Config config = K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map));

    Assertions.assertEquals("https://jobs:6443/", config.getMasterUrl());
    Assertions.assertEquals(caCert.toString(), config.getCaCertFile());
    Assertions.assertEquals("token-1", config.getOauthTokenProvider().getToken());
  }

  @Test
  public void testMissingFiles() throws IOException {
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.MASTER_URL, "https://jobs:6443");
    map.put(K8sJobExecutorConfigs.TOKEN_FILE, dir.resolve("missing").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));

    // A token file without a token fails the startup too, rather than the first request.
    map.put(
        K8sJobExecutorConfigs.TOKEN_FILE,
        Files.write(dir.resolve("empty-token"), new byte[0]).toString());
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));
    Assertions.assertTrue(e.getMessage().contains("No token can be read"), e.getMessage());

    Map<String, String> kubeconfig = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    kubeconfig.put(K8sJobExecutorConfigs.KUBECONFIG, dir.resolve("missing").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(kubeconfig)));
  }

  @Test
  public void testKubeconfig() throws IOException {
    Path kubeconfig =
        Files.write(dir.resolve("config"), KUBECONFIG.getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());

    Config current = K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map));
    Assertions.assertEquals("https://dev:6443/", current.getMasterUrl());

    map.put(K8sJobExecutorConfigs.CONTEXT, "prod");
    Config prod = K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map));
    Assertions.assertEquals("https://prod:6443/", prod.getMasterUrl());
    Assertions.assertEquals("admin-token", prod.getAutoOAuthToken());

    // A context that doesn't exist must not fall back to the current one.
    map.put(K8sJobExecutorConfigs.CONTEXT, "staging");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));
  }

  @Test
  public void testKubeconfigWithoutUsableCluster() throws IOException {
    // Without a context that selects a cluster with a server, the client would silently fall
    // back to the API server of the cluster Gravitino itself runs in.
    String devContext = "context: {cluster: dev, user: admin}";
    String devCluster = "- name: dev\n  cluster: {server: 'https://dev:6443'}\n";
    String[] unusable = {
      // No current context.
      KUBECONFIG.replace("current-context: dev\n", ""),
      // The current context doesn't exist.
      KUBECONFIG.replace("current-context: dev\n", "current-context: staging\n"),
      // The context has no content, or no cluster.
      KUBECONFIG.replace("  " + devContext + "\n", ""),
      KUBECONFIG.replace(devContext, "context: {user: admin}"),
      // The cluster of the context isn't defined.
      KUBECONFIG.replace(devContext, "context: {cluster: nope}"),
      // The cluster is defined without content, with null or empty content, or a blank server.
      KUBECONFIG.replace(devCluster, "- name: dev\n"),
      KUBECONFIG.replace(devCluster, "- name: dev\n  cluster: null\n"),
      KUBECONFIG.replace(devCluster, "- name: dev\n  cluster: {}\n"),
      KUBECONFIG.replace(devCluster, "- name: dev\n  cluster: {server: ' '}\n"),
      // Not a kubeconfig at all.
      "- just\n- a list\n",
    };
    for (String content : unusable) {
      Assertions.assertNotEquals(KUBECONFIG, content);
      Path kubeconfig =
          Files.write(dir.resolve("config"), content.getBytes(StandardCharsets.UTF_8));
      Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
      map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)),
          content);
    }

    // Naming a usable context works, whatever the current context is.
    Path kubeconfig =
        Files.write(
            dir.resolve("config"),
            KUBECONFIG.replace(devCluster, "- name: dev\n").getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
    map.put(K8sJobExecutorConfigs.CONTEXT, "prod");
    Assertions.assertEquals(
        "https://prod:6443/",
        K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());
  }

  @Test
  public void testKubeconfigSelectingDefaultServer() throws IOException {
    // A kubeconfig may well select the default API server of the client.
    Path kubeconfig =
        Files.write(
            dir.resolve("config"),
            KUBECONFIG
                .replace("https://dev:6443", "https://kubernetes.default.svc")
                .getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
    Assertions.assertEquals(
        "https://kubernetes.default.svc/",
        K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());
  }

  @Test
  public void testKubeconfigServerWithoutScheme() throws IOException {
    // The client picks the scheme of such a server by probing it, whatever it picks is accepted.
    // Nothing listens on the port, so the probe fails right away.
    Path kubeconfig =
        Files.write(
            dir.resolve("config"),
            KUBECONFIG.replace("https://dev:6443", "localhost:1").getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
    String masterUrl =
        K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl();
    Assertions.assertTrue(masterUrl.endsWith("://localhost:1/"), masterUrl);

    Files.write(
        kubeconfig,
        KUBECONFIG
            .replace("https://dev:6443", "HTTPS://dev:6443/")
            .getBytes(StandardCharsets.UTF_8));
    // The scheme is compared ignoring its case, and the client keeps the server as written.
    Assertions.assertEquals(
        "HTTPS://dev:6443/",
        K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());
  }

  @Test
  public void testKubeconfigWithDuplicateClusters() throws IOException {
    // Like the client, the last cluster with a name wins.
    Path kubeconfig =
        Files.write(
            dir.resolve("config"),
            KUBECONFIG
                .replace(
                    "- name: prod\n",
                    "- name: dev\n  cluster: {server: 'https://dev2:6443'}\n- name: prod\n")
                .getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
    Assertions.assertEquals(
        "https://dev2:6443/",
        K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());
  }

  @Test
  public void testEmptyKubeconfig() throws IOException {
    for (String content : new String[] {"", "# nothing\n", "---\n"}) {
      Path kubeconfig =
          Files.write(dir.resolve("config"), content.getBytes(StandardCharsets.UTF_8));
      Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
      map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)),
          content);
    }
  }

  @Test
  public void testDefaultLookupWithoutUsableCluster() throws IOException {
    // The client prefers this system property over the KUBECONFIG environment variable.
    String property = "kubeconfig";
    String before = System.getProperty(property);
    Path kubeconfig = dir.resolve("config");
    System.setProperty(property, kubeconfig.toString());
    try {
      Files.write(kubeconfig, KUBECONFIG.getBytes(StandardCharsets.UTF_8));
      Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
      Assertions.assertEquals(
          "https://dev:6443/",
          K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());

      // The cluster of the context isn't defined, so the client would stay on the API server of
      // the cluster Gravitino itself runs in.
      Files.write(
          kubeconfig,
          KUBECONFIG
              .replace("context: {cluster: dev, user: admin}", "context: {cluster: nope}")
              .getBytes(StandardCharsets.UTF_8));
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));
      Assertions.assertTrue(e.getMessage().contains("selects no cluster"), e.getMessage());

      map.put(K8sJobExecutorConfigs.CONTEXT, "dev");
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));

      map.put(K8sJobExecutorConfigs.CONTEXT, "prod");
      Assertions.assertEquals(
          "https://prod:6443/",
          K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());

      // A context that doesn't exist is reported as such, even if the current context that the
      // client falls back to is broken too.
      map.put(K8sJobExecutorConfigs.CONTEXT, "staging");
      e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));
      Assertions.assertTrue(e.getMessage().contains("The context staging"), e.getMessage());

      // No context is selected: no current context, or one that doesn't exist.
      map.remove(K8sJobExecutorConfigs.CONTEXT);
      for (String content :
          new String[] {
            KUBECONFIG.replace("current-context: dev\n", ""),
            KUBECONFIG.replace("current-context: dev\n", "current-context: staging\n")
          }) {
        Files.write(kubeconfig, content.getBytes(StandardCharsets.UTF_8));
        e =
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)),
                content);
        Assertions.assertTrue(e.getMessage().contains("has no current context"), e.getMessage());
      }

      // Without any kubeconfig, and outside a pod, no cluster is configured at all.
      System.setProperty(property, dir.resolve("missing").toString());
      e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));
      Assertions.assertTrue(e.getMessage().contains("No Kubernetes cluster"), e.getMessage());

      // In a pod, the kubelet sets the API server through these, which the client reads from
      // the system properties first.
      System.setProperty("KUBERNETES_SERVICE_HOST", "10.0.0.1");
      System.setProperty("KUBERNETES_SERVICE_PORT", "443");
      try {
        Assertions.assertEquals(
            "https://10.0.0.1:443/",
            K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getMasterUrl());
      } finally {
        System.clearProperty("KUBERNETES_SERVICE_HOST");
        System.clearProperty("KUBERNETES_SERVICE_PORT");
      }
    } finally {
      if (before == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, before);
      }
    }
  }

  @Test
  public void testRequestLimits() throws IOException {
    // The limits are set in every connection mode.
    Path kubeconfig =
        Files.write(dir.resolve("config"), KUBECONFIG.getBytes(StandardCharsets.UTF_8));
    Map<String, String> withKubeconfig = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    withKubeconfig.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
    Map<String, String> withMasterUrl = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    withMasterUrl.put(K8sJobExecutorConfigs.MASTER_URL, "https://jobs:6443");
    for (Map<String, String> map : ImmutableList.of(withKubeconfig, withMasterUrl)) {
      Config defaults = K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map));
      Assertions.assertEquals(5_000, defaults.getConnectionTimeout());
      Assertions.assertEquals(10_000, defaults.getRequestTimeout());
      Assertions.assertEquals(2, defaults.getRequestRetryBackoffLimit());

      map.put(K8sJobExecutorConfigs.CONNECTION_TIMEOUT_MS, "1000");
      map.put(K8sJobExecutorConfigs.REQUEST_TIMEOUT_MS, "2000");
      map.put(K8sJobExecutorConfigs.REQUEST_RETRIES, "0");
      Config custom = K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map));
      Assertions.assertEquals(1_000, custom.getConnectionTimeout());
      Assertions.assertEquals(2_000, custom.getRequestTimeout());
      Assertions.assertEquals(0, custom.getRequestRetryBackoffLimit());
    }
  }

  @Test
  public void testRequestFailsFastWithoutApiServer() {
    // Nothing listens on the port. With the client's own defaults this takes about 20 seconds.
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.MASTER_URL, "https://127.0.0.1:1");
    try (KubernetesClient client = K8sClientUtils.createClient(new K8sJobExecutorConfigs(map))) {
      Assertions.assertTimeout(
          Duration.ofSeconds(10),
          () ->
              Assertions.assertThrows(
                  KubernetesClientException.class,
                  () -> client.pods().inNamespace("default").list()));
    }
  }

  @Test
  public void testNamespace() throws IOException {
    // The configured namespace wins over the one of the kubeconfig context.
    Path kubeconfig =
        Files.write(
            dir.resolve("config"),
            KUBECONFIG
                .replace(
                    "{cluster: dev, user: admin}", "{cluster: dev, user: admin, namespace: ctx}")
                .getBytes(StandardCharsets.UTF_8));
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.KUBECONFIG, kubeconfig.toString());
    map.put(K8sJobExecutorConfigs.NAMESPACE, "jobs");
    Assertions.assertEquals(
        "jobs", K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)).getNamespace());

    Map<String, String> masterUrl = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    masterUrl.put(K8sJobExecutorConfigs.MASTER_URL, "https://jobs:6443");
    masterUrl.put(K8sJobExecutorConfigs.NAMESPACE, "jobs");
    Assertions.assertEquals(
        "jobs",
        K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(masterUrl)).getNamespace());
  }

  @Test
  public void testTokenRotation() throws IOException {
    Path token = Files.write(dir.resolve("token"), "token-1".getBytes(StandardCharsets.UTF_8));
    K8sClientUtils.FileTokenProvider provider = new K8sClientUtils.FileTokenProvider(token);
    Assertions.assertEquals("token-1", provider.getToken());

    Files.write(token, "token-2".getBytes(StandardCharsets.UTF_8));
    // Make sure the modification time changes, even on file systems with a coarse resolution.
    Files.setLastModifiedTime(
        token, FileTime.fromMillis(Files.getLastModifiedTime(token).toMillis() + 1000));
    Assertions.assertEquals("token-2", provider.getToken());

    // The token read before is kept while the file is empty or missing, as it is being rewritten.
    Files.write(token, new byte[0]);
    Files.setLastModifiedTime(
        token, FileTime.fromMillis(Files.getLastModifiedTime(token).toMillis() + 2000));
    Assertions.assertEquals("token-2", provider.getToken());
    Files.delete(token);
    Assertions.assertEquals("token-2", provider.getToken());

    Files.write(token, "token-3".getBytes(StandardCharsets.UTF_8));
    Assertions.assertEquals("token-3", provider.getToken());
  }

  @Test
  public void testTokenRotationWithinOneModificationTime() throws IOException {
    // The file is emptied and rewritten within one tick of a coarse file system clock.
    Path token = Files.write(dir.resolve("token"), "token-1".getBytes(StandardCharsets.UTF_8));
    K8sClientUtils.FileTokenProvider provider = new K8sClientUtils.FileTokenProvider(token);
    Assertions.assertEquals("token-1", provider.getToken());

    FileTime rotatedAt = FileTime.fromMillis(Files.getLastModifiedTime(token).toMillis() + 1000);
    Files.write(token, new byte[0]);
    Files.setLastModifiedTime(token, rotatedAt);
    Assertions.assertEquals("token-1", provider.getToken());

    Files.write(token, "token-2".getBytes(StandardCharsets.UTF_8));
    Files.setLastModifiedTime(token, rotatedAt);
    Assertions.assertEquals("token-2", provider.getToken());
  }

  @Test
  public void testUnreadableTokenFile() throws IOException {
    Path empty = Files.write(dir.resolve("empty"), new byte[0]);
    Assertions.assertThrows(
        IllegalStateException.class, new K8sClientUtils.FileTokenProvider(empty)::getToken);
    Assertions.assertThrows(
        UncheckedIOException.class,
        new K8sClientUtils.FileTokenProvider(dir.resolve("missing"))::getToken);
  }
}
