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
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates the Kubernetes client of the job cluster from the job executor configurations. */
public final class K8sClientUtils {

  /**
   * An {@link OAuthTokenProvider} that reads the token from a file, and reads it again whenever the
   * file changes, so that a rotated token is picked up without restarting the server.
   *
   * <p>It doesn't refresh the token itself, whoever provides the file does. For a file mounted from
   * a Secret, that is whatever updates the Secret, and the kubelet then updates the mounted file,
   * unless it is mounted with a {@code subPath}. For a projected service account token, the kubelet
   * rotates it by itself.
   */
  static final class FileTokenProvider implements OAuthTokenProvider {

    // How often a token file that stays empty or unreadable is reported.
    private static final long FAILURE_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);

    private final Path tokenFile;

    private FileTime lastModified;

    private String token;

    private boolean failing;

    private long failureLoggedAtNanos;

    FileTokenProvider(Path tokenFile) {
      this.tokenFile = tokenFile;
    }

    @Override
    public synchronized String getToken() {
      try {
        FileTime modified = Files.getLastModifiedTime(tokenFile);
        if (token == null || !modified.equals(lastModified)) {
          String content = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8).trim();
          if (StringUtils.isBlank(content)) {
            if (token == null) {
              throw new IllegalStateException("The token file " + tokenFile + " is empty");
            }
            // The file is being rewritten. Its modification time isn't remembered: the new token
            // may be written within the same tick of the file system's clock, and would then
            // never be read.
            return keepToken("is empty", null);
          }
          token = content;
          lastModified = modified;
        }
      } catch (IOException e) {
        if (token == null) {
          throw new UncheckedIOException("Failed to read the token file " + tokenFile, e);
        }
        // For example, the file is being replaced.
        return keepToken("can't be read", e);
      }
      failing = false;
      return token;
    }

    /**
     * Keeps the token read before while the file has no token, checking the file again on the next
     * request. The problem is reported when it starts, and then at an interval rather than on every
     * request, so that a file that stays broken remains visible without flooding the log.
     */
    private String keepToken(String problem, @Nullable IOException cause) {
      long now = System.nanoTime();
      if (!failing || now - failureLoggedAtNanos >= FAILURE_LOG_INTERVAL_NANOS) {
        LOG.warn(
            "The token file {} {}, keeping the token read before. Requests will fail once that"
                + " token expires",
            tokenFile,
            problem,
            cause);
        failing = true;
        failureLoggedAtNanos = now;
      }
      return token;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(K8sClientUtils.class);

  private static final Pattern SERVER_SCHEME =
      Pattern.compile("^https?://", Pattern.CASE_INSENSITIVE);

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
   * The connection timeout, the request timeout and the number of request retries are the ones of
   * the job executor configurations too. They bound how long a call to the client waits while the
   * job cluster is down: each of the 1 + retries attempts waits for the connection timeout if the
   * API server can't be reached, and for the request timeout if it accepts the request but never
   * answers. With the defaults that is about 15 and 30 seconds.
   *
   * @param configs the job executor configurations
   * @return the Kubernetes client configuration
   * @throws IllegalArgumentException if a configured file doesn't exist, the token file has no
   *     token, the kubeconfig has no context that selects a cluster with a server, or the default
   *     lookup finds no cluster at all
   */
  static Config createClientConfig(K8sJobExecutorConfigs configs) {
    Config config = createConnectionConfig(configs);
    config.setNamespace(configs.namespace());
    // The client would otherwise try a request again 10 times, which holds a status query or a
    // job submission for about two minutes while the job cluster can't be reached.
    config.setConnectionTimeout(configs.connectionTimeoutMs());
    config.setRequestTimeout(configs.requestTimeoutMs());
    config.setRequestRetryBackoffLimit(configs.requestRetries());
    return config;
  }

  private static Config createConnectionConfig(K8sJobExecutorConfigs configs) {
    if (configs.masterUrl() != null) {
      ConfigBuilder builder = new ConfigBuilder(Config.empty()).withMasterUrl(configs.masterUrl());
      if (configs.caCertFile() != null) {
        checkFileExists(configs.caCertFile(), K8sJobExecutorConfigs.CA_CERT_FILE);
        builder.withCaCertFile(configs.caCertFile());
      }
      if (configs.tokenFile() != null) {
        checkFileExists(configs.tokenFile(), K8sJobExecutorConfigs.TOKEN_FILE);
        FileTokenProvider tokenProvider = new FileTokenProvider(Paths.get(configs.tokenFile()));
        // Read the token once now, so that a file without a token fails the startup rather than
        // the first request.
        try {
          tokenProvider.getToken();
        } catch (IllegalStateException | UncheckedIOException e) {
          throw new IllegalArgumentException(
              String.format(
                  "No token can be read from the file %s set by %s",
                  configs.tokenFile(), K8sJobExecutorConfigs.TOKEN_FILE),
              e);
        }
        builder.withOauthTokenProvider(tokenProvider);
      }
      return builder.build();
    }

    Config config;
    if (configs.kubeconfig() != null) {
      checkFileExists(configs.kubeconfig(), K8sJobExecutorConfigs.KUBECONFIG);
      File kubeconfig = new File(configs.kubeconfig());
      String server = K8sKubeconfigUtils.getSelectedServer(kubeconfig, configs.context());
      config = Config.fromKubeconfig(configs.context(), kubeconfig);
      // The client reads the file itself, so make sure it ended up with the server found above.
      // This is a safety net with no known input that triggers it: it only fails if the client
      // reads a kubeconfig differently from the check above, or the file changes in between.
      Preconditions.checkArgument(
          getHostAndPath(server).equals(getHostAndPath(config.getMasterUrl())),
          "The kubeconfig %s set by %s selects the server %s, but the client would use %s",
          configs.kubeconfig(),
          K8sJobExecutorConfigs.KUBECONFIG,
          server,
          config.getMasterUrl());
    } else {
      config = Config.autoConfigure(configs.context());
      checkDefaultLookup(config, configs);
    }
    return config;
  }

  /**
   * Checks the client configuration that the default lookup found. The lookup may merge several
   * kubeconfig files, so they aren't checked one by one like a configured kubeconfig. Instead, the
   * result must not be one of those where the client silently stays on its default API server, the
   * one of the cluster Gravitino runs in, although a kubeconfig was found.
   */
  private static void checkDefaultLookup(Config config, K8sJobExecutorConfigs configs) {
    NamedContext current = config.getCurrentContext();
    String currentName = current == null ? null : current.getName();
    // The client falls back to the current context if the given one doesn't exist. Checked
    // first, so that the error is about the context that was asked for.
    Preconditions.checkArgument(
        configs.context() == null || configs.context().equals(currentName),
        "The context %s set by %s doesn't exist in the kubeconfig found by the default lookup",
        configs.context(),
        K8sJobExecutorConfigs.CONTEXT);

    // A kubeconfig with contexts was found, but none of them is selected. Without any
    // kubeconfig, as in a pod, there are no contexts and the service account is used.
    Preconditions.checkArgument(
        current != null || config.getContexts() == null || config.getContexts().isEmpty(),
        "The kubeconfig found by the default lookup has no current context, set its"
            + " current-context or %s",
        K8sJobExecutorConfigs.CONTEXT);

    // Both a context that selects a cluster with a server, and the environment of a pod, move
    // the client off its default API server. If it is still there, the lookup found neither.
    boolean foundServer =
        !getHostAndPath(Config.DEFAULT_MASTER_URL).equals(getHostAndPath(config.getMasterUrl()));
    Preconditions.checkArgument(
        foundServer || current == null,
        "The context %s of the kubeconfig found by the default lookup selects no cluster with a"
            + " server. Fix that kubeconfig, or set %s or %s. To use the API server %s itself,"
            + " set it by %s",
        currentName,
        K8sJobExecutorConfigs.KUBECONFIG,
        K8sJobExecutorConfigs.MASTER_URL,
        Config.DEFAULT_MASTER_URL,
        K8sJobExecutorConfigs.MASTER_URL);
    Preconditions.checkArgument(
        foundServer,
        "No Kubernetes cluster is configured: the default lookup found no kubeconfig, and"
            + " Gravitino doesn't run in a pod. Set %s or %s. To use the API server %s itself,"
            + " set it by %s",
        K8sJobExecutorConfigs.KUBECONFIG,
        K8sJobExecutorConfigs.MASTER_URL,
        Config.DEFAULT_MASTER_URL,
        K8sJobExecutorConfigs.MASTER_URL);
  }

  /**
   * Returns the URL of an API server without its scheme and trailing slashes, to compare the server
   * of a kubeconfig with the one the client uses. The scheme is left out because the client picks
   * it for a server without one, by probing whether the server speaks HTTPS.
   */
  private static String getHostAndPath(String server) {
    return SERVER_SCHEME.matcher(server.trim()).replaceFirst("").replaceAll("/+$", "");
  }

  private static void checkFileExists(String path, String key) {
    Preconditions.checkArgument(
        new File(path).isFile(), "The file %s set by %s doesn't exist", path, key);
  }
}
