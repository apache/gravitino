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

import io.fabric8.kubernetes.client.Config;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
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
  public void testMissingFiles() {
    Map<String, String> map = new HashMap<>(TestK8sJobExecutorConfigs.requiredConfigs());
    map.put(K8sJobExecutorConfigs.MASTER_URL, "https://jobs:6443");
    map.put(K8sJobExecutorConfigs.TOKEN_FILE, dir.resolve("missing").toString());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sClientUtils.createClientConfig(new K8sJobExecutorConfigs(map)));

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
  public void testUnreadableTokenFile() throws IOException {
    Path empty = Files.write(dir.resolve("empty"), new byte[0]);
    Assertions.assertThrows(
        IllegalStateException.class, new K8sClientUtils.FileTokenProvider(empty)::getToken);
    Assertions.assertThrows(
        UncheckedIOException.class,
        new K8sClientUtils.FileTokenProvider(dir.resolve("missing"))::getToken);
  }
}
