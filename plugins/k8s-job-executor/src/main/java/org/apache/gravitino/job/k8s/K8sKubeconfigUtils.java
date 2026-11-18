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
import io.fabric8.kubernetes.api.model.Cluster;
import io.fabric8.kubernetes.api.model.Config;
import io.fabric8.kubernetes.api.model.Context;
import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.client.utils.Serialization;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Reads a kubeconfig file into the model of the Kubernetes client, to check what the client will
 * make of it.
 */
final class K8sKubeconfigUtils {

  private K8sKubeconfigUtils() {}

  /**
   * Returns the API server of the cluster that the kubeconfig selects, and fails if it selects
   * none. Without one the client keeps its default API server, the one of the cluster Gravitino
   * runs in, and would silently run the jobs there.
   *
   * @param kubeconfig the kubeconfig file, set by {@link K8sJobExecutorConfigs#KUBECONFIG}
   * @param contextName the context to use, or null for the current context of the kubeconfig
   * @return the server of the cluster of the context
   * @throws IllegalArgumentException if the kubeconfig can't be parsed, or its context doesn't
   *     select a cluster with a server
   */
  static String getSelectedServer(File kubeconfig, @Nullable String contextName) {
    Config content;
    try (InputStream in = Files.newInputStream(kubeconfig.toPath())) {
      content = Serialization.unmarshal(in, Config.class);
    } catch (IOException | RuntimeException e) {
      throw new IllegalArgumentException(
          String.format(
              "The kubeconfig %s set by %s can't be parsed",
              kubeconfig, K8sJobExecutorConfigs.KUBECONFIG),
          e);
    }
    // An empty file has no content at all.
    Preconditions.checkArgument(
        content != null,
        "The kubeconfig %s set by %s is empty",
        kubeconfig,
        K8sJobExecutorConfigs.KUBECONFIG);

    String name = contextName != null ? contextName : content.getCurrentContext();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name),
        "The kubeconfig %s set by %s has no current context, set its current-context or %s",
        kubeconfig,
        K8sJobExecutorConfigs.KUBECONFIG,
        K8sJobExecutorConfigs.CONTEXT);

    // Like the client, the last entry with a name wins, and an entry without content is skipped.
    Context context = null;
    if (content.getContexts() != null) {
      for (NamedContext entry : content.getContexts()) {
        if (entry != null && name.equals(entry.getName()) && entry.getContext() != null) {
          context = entry.getContext();
        }
      }
    }
    Preconditions.checkArgument(
        context != null,
        "The context %s doesn't exist in the kubeconfig %s set by %s",
        name,
        kubeconfig,
        K8sJobExecutorConfigs.KUBECONFIG);

    Cluster cluster = null;
    if (content.getClusters() != null && context.getCluster() != null) {
      for (NamedCluster entry : content.getClusters()) {
        if (entry != null
            && context.getCluster().equals(entry.getName())
            && entry.getCluster() != null) {
          cluster = entry.getCluster();
        }
      }
    }
    Preconditions.checkArgument(
        cluster != null && StringUtils.isNotBlank(cluster.getServer()),
        "The cluster %s of the context %s has no server in the kubeconfig %s set by %s",
        context.getCluster(),
        name,
        kubeconfig,
        K8sJobExecutorConfigs.KUBECONFIG);
    return cluster.getServer();
  }
}
