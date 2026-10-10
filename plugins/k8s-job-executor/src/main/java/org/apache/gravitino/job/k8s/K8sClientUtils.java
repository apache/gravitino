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
import io.fabric8.kubernetes.api.model.NamedContext;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        // For example, the file is being replaced, keep the token until the new one is there.
        LOG.warn("Failed to read the token file {}, keeping the token read before", tokenFile, e);
      }
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
      File kubeconfig = new File(configs.kubeconfig());
      config = Config.fromKubeconfig(configs.context(), kubeconfig);
      // Without a context whose cluster is defined, the client keeps its default API server,
      // the one of the cluster Gravitino runs in, and would silently run the jobs there.
      NamedContext context = config.getCurrentContext();
      Preconditions.checkArgument(
          context != null
              && context.getContext() != null
              && getClusterNames(kubeconfig).contains(context.getContext().getCluster()),
          "The kubeconfig %s set by %s has no usable context: set its current-context or %s, and"
              + " make sure the cluster of the context is defined",
          configs.kubeconfig(),
          K8sJobExecutorConfigs.KUBECONFIG,
          K8sJobExecutorConfigs.CONTEXT);
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

  /** Returns the names of the clusters that the kubeconfig file defines. */
  @SuppressWarnings("unchecked")
  private static Set<String> getClusterNames(File kubeconfig) {
    Map<String, Object> content;
    try {
      content =
          Serialization.unmarshal(
              new String(Files.readAllBytes(kubeconfig.toPath()), StandardCharsets.UTF_8),
              Map.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the kubeconfig " + kubeconfig, e);
    }

    Set<String> names = new HashSet<>();
    Object clusters = content == null ? null : content.get("clusters");
    if (clusters instanceof List) {
      for (Object cluster : (List<Object>) clusters) {
        if (cluster instanceof Map
            && ((Map<String, Object>) cluster).get("name") instanceof String) {
          names.add((String) ((Map<String, Object>) cluster).get("name"));
        }
      }
    }
    return names;
  }

  private static void checkFileExists(String path, String key) {
    Preconditions.checkArgument(
        new File(path).isFile(), "The file %s set by %s doesn't exist", path, key);
  }
}
