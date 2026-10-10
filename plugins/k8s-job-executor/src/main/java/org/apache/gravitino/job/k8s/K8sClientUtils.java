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
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
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
            // The file is being rewritten, keep the token until the new one is there.
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
   * @param configs the job executor configurations
   * @return the Kubernetes client configuration
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
      return builder.build();
    }

    Config config;
    if (configs.kubeconfig() != null) {
      checkFileExists(configs.kubeconfig(), K8sJobExecutorConfigs.KUBECONFIG);
      config = Config.fromKubeconfig(configs.context(), new File(configs.kubeconfig()));
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
    return config;
  }

  private static void checkFileExists(String path, String key) {
    Preconditions.checkArgument(
        new File(path).isFile(), "The file %s set by %s doesn't exist", path, key);
  }
}
