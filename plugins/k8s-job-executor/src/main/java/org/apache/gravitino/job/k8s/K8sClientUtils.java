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

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.OAuthTokenProvider;
import io.fabric8.kubernetes.client.utils.Serialization;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates the Kubernetes client of the job cluster from the job executor configurations. */
public final class K8sClientUtils {

  /**
   * An {@link OAuthTokenProvider} that reads the token from a file, and reads it again whenever the
   * file changes, so that a rotated token is picked up without restarting the server.
   */
  static final class FileTokenProvider implements OAuthTokenProvider {

    private final Path tokenFile;

    private FileTime lastModified;

    private String token;

    private boolean readFailureLogged;

    FileTokenProvider(Path tokenFile) {
      this.tokenFile = tokenFile;
    }

    @Override
    public synchronized String getToken() {
      try {
        FileTime modified = Files.getLastModifiedTime(tokenFile);
        if (token == null || !modified.equals(lastModified)) {
          String content = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8).trim();
          if (StringUtils.isNotBlank(content)) {
            token = content;
            lastModified = modified;
          } else if (token == null) {
            throw new IllegalStateException("The token file " + tokenFile + " is empty");
          } else {
            // The file is being rewritten, keep the token until the new one is there. Remember
            // this version of the file, so that it is neither read nor reported again.
            lastModified = modified;
            LOG.warn("The token file {} is empty, keeping the token read before", tokenFile);
          }
        }
      } catch (IOException e) {
        if (token == null) {
          throw new UncheckedIOException("Failed to read the token file " + tokenFile, e);
        }
        // For example, the file is being replaced, keep the token until the new one is there. It
        // is checked again on every request, so only the first failure in a row is reported.
        if (!readFailureLogged) {
          LOG.warn("Failed to read the token file {}, keeping the token read before", tokenFile, e);
          readFailureLogged = true;
        }
        return token;
      }
      readFailureLogged = false;
      return token;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(K8sClientUtils.class);

  private K8sClientUtils() {}

  /**
   * Creates the Kubernetes client of the job cluster.
   *
   * @param configs the job executor configurations
   * @return the Kubernetes client
   */
  public static KubernetesClient createClient(K8sJobExecutorConfigs configs) {
    return new KubernetesClientBuilder().withConfig(createClientConfig(configs)).build();
  }

  /**
   * Creates the Kubernetes client configuration of the job cluster. It is, by precedence:
   *
   * <ul>
   *   <li>the API server URL, with its CA certificate and token file, if the master URL is set;
   *   <li>the given kubeconfig file and context, if the kubeconfig is set;
   *   <li>fabric8's default lookup otherwise, that is the {@code KUBECONFIG} environment variable
   *       or {@code ~/.kube/config} with the given context, then the in-cluster service account.
   * </ul>
   *
   * <p>In every case the default namespace of the client is the namespace of the job executor
   * configurations, rather than the one of the kubeconfig context or of the pod Gravitino runs in.
   *
   * @param configs the job executor configurations
   * @return the Kubernetes client configuration
   * @throws IllegalArgumentException if a configured file doesn't exist, or the kubeconfig has no
   *     usable context
   */
  static Config createClientConfig(K8sJobExecutorConfigs configs) {
    if (configs.masterUrl() != null) {
      ConfigBuilder builder = new ConfigBuilder(Config.empty()).withMasterUrl(configs.masterUrl());
      if (configs.caCertFile() != null) {
        checkFileExists(configs.caCertFile(), K8sJobExecutorConfigs.CA_CERT_FILE);
        builder.withCaCertFile(configs.caCertFile());
      }
      if (configs.tokenFile() != null) {
        checkFileExists(configs.tokenFile(), K8sJobExecutorConfigs.TOKEN_FILE);
        builder.withOauthTokenProvider(new FileTokenProvider(Paths.get(configs.tokenFile())));
      }
      return builder.withNamespace(configs.namespace()).build();
    }

    Config config;
    if (configs.kubeconfig() != null) {
      checkFileExists(configs.kubeconfig(), K8sJobExecutorConfigs.KUBECONFIG);
      String server = getKubeconfigServer(configs);
      config = Config.fromKubeconfig(configs.context(), new File(configs.kubeconfig()));
      // The client reads the file itself, so make sure it ended up with the server found above,
      // whatever it does with a kubeconfig that is malformed or changed in between.
      Preconditions.checkArgument(
          normalizeServer(server).equals(normalizeServer(config.getMasterUrl())),
          "The kubeconfig %s set by %s selects the server %s, but the client would use %s",
          configs.kubeconfig(),
          K8sJobExecutorConfigs.KUBECONFIG,
          server,
          config.getMasterUrl());
    } else {
      config = Config.autoConfigure(configs.context());
    }

    // The client falls back to the current context if the given one doesn't exist, which would
    // silently run the jobs in another cluster.
    String context =
        config.getCurrentContext() == null ? null : config.getCurrentContext().getName();
    Preconditions.checkArgument(
        configs.context() == null || configs.context().equals(context),
        "The context %s set by %s doesn't exist in the kubeconfig",
        configs.context(),
        K8sJobExecutorConfigs.CONTEXT);
    config.setNamespace(configs.namespace());
    return config;
  }

  /**
   * Returns the API server of the cluster that the kubeconfig selects, and fails if it selects
   * none. Otherwise the client keeps its default API server, the one of the cluster Gravitino runs
   * in, and would silently run the jobs there. The default API server itself isn't rejected, as a
   * kubeconfig may well select it.
   */
  private static String getKubeconfigServer(K8sJobExecutorConfigs configs) {
    Map<String, Object> kubeconfig;
    try {
      kubeconfig =
          getMap(
              Serialization.unmarshal(
                  new String(
                      Files.readAllBytes(Paths.get(configs.kubeconfig())), StandardCharsets.UTF_8),
                  Map.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the kubeconfig " + configs.kubeconfig(), e);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          String.format(
              "The kubeconfig %s set by %s can't be parsed",
              configs.kubeconfig(), K8sJobExecutorConfigs.KUBECONFIG),
          e);
    }

    Object contextName =
        configs.context() != null
            ? configs.context()
            : kubeconfig == null ? null : kubeconfig.get("current-context");
    Preconditions.checkArgument(
        contextName instanceof String && StringUtils.isNotBlank((String) contextName),
        "The kubeconfig %s set by %s has no current context, set its current-context or %s",
        configs.kubeconfig(),
        K8sJobExecutorConfigs.KUBECONFIG,
        K8sJobExecutorConfigs.CONTEXT);

    Map<String, Object> context = getNamedEntry(kubeconfig, "contexts", contextName, "context");
    Preconditions.checkArgument(
        context != null,
        "The context %s doesn't exist in the kubeconfig %s set by %s",
        contextName,
        configs.kubeconfig(),
        K8sJobExecutorConfigs.KUBECONFIG);

    Object clusterName = context.get("cluster");
    Map<String, Object> cluster = getNamedEntry(kubeconfig, "clusters", clusterName, "cluster");
    Object server = cluster == null ? null : cluster.get("server");
    Preconditions.checkArgument(
        server instanceof String && StringUtils.isNotBlank((String) server),
        "The cluster %s of the context %s has no server in the kubeconfig %s set by %s",
        clusterName,
        contextName,
        configs.kubeconfig(),
        K8sJobExecutorConfigs.KUBECONFIG);
    return (String) server;
  }

  /** Returns the URL of an API server in the form the client keeps it, to compare two of them. */
  private static String normalizeServer(String server) {
    String url = server.trim();
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
      url = "https://" + url;
    }
    return url.endsWith("/") ? url : url + "/";
  }

  /**
   * Returns the content of the entry with the given name in a list of the kubeconfig, for example
   * the {@code cluster} of the entry named {@code dev} in {@code clusters}.
   */
  @Nullable
  private static Map<String, Object> getNamedEntry(
      @Nullable Map<String, Object> kubeconfig, String list, Object name, String content) {
    Object entries = kubeconfig == null ? null : kubeconfig.get(list);
    if (!(entries instanceof List) || name == null) {
      return null;
    }
    for (Object entry : (List<?>) entries) {
      Map<String, Object> fields = getMap(entry);
      if (fields != null && name.equals(fields.get("name"))) {
        return getMap(fields.get(content));
      }
    }
    return null;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> getMap(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : null;
  }

  private static void checkFileExists(String path, String key) {
    Preconditions.checkArgument(
        new File(path).isFile(), "The file %s set by %s doesn't exist", path, key);
  }
}
