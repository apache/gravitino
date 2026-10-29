/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.ITUtils;

/**
 * One Trino node of the Gravitino CI Trino image ({@link TestImages#TRINO}) that is both
 * coordinator and worker, talking to one Gravitino server and metalake and to the run's shared
 * Hive2. Unlike a {@link TrinoCluster}, it brings no services of its own.
 *
 * <p>The node reads the metalake when it starts, so a test creates the metalake and its catalogs
 * first and then asks for the node:
 *
 * <pre>{@code
 * TrinoContainer trino = Fixtures.get(new TrinoNodeSpec(metalakeName, getGravitinoServerPort()));
 * }</pre>
 *
 * <p>The connector comes from {@code trino-connector/trino-connector-473-478/build/libs}, plus
 * every jar in the directory {@code GRAVITINO_TRINO_CONNECTOR_EXTRA_LIBS} names; {@code
 * TRINO_CONF_DIR}, when set, replaces files under {@code /etc/trino}.
 */
public record TrinoNodeSpec(String metalake, int gravitinoServerPort)
    implements ContainerSpec<TrinoContainer> {

  /** The Hive the node's catalogs point at, which the node maps to its shared host name. */
  static final HiveSpec HIVE = new HiveSpec(HiveVersion.HIVE2, HiveAuth.NONE);

  /**
   * Creates the spec.
   *
   * @param metalake the metalake the node's Gravitino connector serves
   * @param gravitinoServerPort the port of the Gravitino server on the host
   */
  public TrinoNodeSpec {
    requireNonNull(metalake, "metalake is null");
    if (gravitinoServerPort <= 0) {
      throw new IllegalArgumentException("The Gravitino server port is required");
    }
  }

  @Override
  public Class<TrinoContainer> containerType() {
    return TrinoContainer.class;
  }

  @Override
  public List<ContainerSpec<?>> dependencies() {
    return List.of(HIVE);
  }

  @Override
  public TrinoContainer getOrStart(SharedContainers shared) {
    return shared.startTrinoNode(this, () -> builder(HIVE.getOrStart(shared)));
  }

  // Nothing looks the node up by name, so a private node keeps the shared node's host name.
  @Override
  public boolean supportsPrivate() {
    return true;
  }

  @Override
  public TrinoContainer startPrivate(SharedContainers shared, Durability durability) {
    return shared.startPrivate(builder(HIVE.getOrStart(shared)), durability);
  }

  /**
   * Returns the builder of the node, without its network, for the given running Hive.
   *
   * @param hive the running Hive
   * @return the builder
   */
  TrinoContainer.Builder builder(HiveContainer hive) {
    return builder(hive.getContainerIpAddress(), connectorDir(), System.getenv("TRINO_CONF_DIR"));
  }

  TrinoContainer.Builder builder(String hiveIp, String connectorDir, @Nullable String confDir) {
    return TrinoContainer.builder()
        .withEnvVars(
            ImmutableMap.of(
                "HADOOP_USER_NAME",
                "anonymous",
                "GRAVITINO_HOST_IP",
                "host.docker.internal",
                "GRAVITINO_HOST_PORT",
                String.valueOf(gravitinoServerPort),
                "GRAVITINO_METALAKE_NAME",
                metalake))
        // The connector calls the server on the host; the image's catalogs name the shared Hive.
        .withExtraHosts(
            ImmutableMap.of(
                "host.docker.internal", "host-gateway", HiveContainer.HOST_NAME, hiveIp))
        .withFilesToMount(
            pluginFiles(connectorDir, System.getenv("GRAVITINO_TRINO_CONNECTOR_EXTRA_LIBS")))
        .withExposePorts(ImmutableSet.of(TrinoContainer.TRINO_PORT))
        .withTrinoConfDir(confDir)
        .withMetalakeName(metalake)
        .withHiveContainerIP(hiveIp);
  }

  /**
   * Returns the files copied into the node's Gravitino plugin directory: the connector directory,
   * plus every jar in {@code extraLibDir}, which lets a build add plugin jars that are not part of
   * the connector itself.
   *
   * @param connectorDir the connector directory on the host
   * @param extraLibDir the directory of extra jars, or null for none
   * @return the files, keyed by container path
   */
  static Map<String, String> pluginFiles(String connectorDir, @Nullable String extraLibDir) {
    ImmutableMap.Builder<String, String> files = ImmutableMap.builder();
    files.put(TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR, connectorDir);
    if (extraLibDir != null && !extraLibDir.isEmpty()) {
      File[] jars = new File(extraLibDir).listFiles((dir, name) -> name.endsWith(".jar"));
      for (File jar : jars == null ? new File[0] : jars) {
        files.put(
            TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR + "/" + jar.getName(),
            jar.getAbsolutePath());
      }
    }
    return files.build();
  }

  private static String connectorDir() {
    String root = System.getenv("GRAVITINO_ROOT_DIR");
    if (root == null || root.isEmpty()) {
      throw new IllegalStateException("GRAVITINO_ROOT_DIR is not set");
    }
    return ITUtils.joinPath(root, "trino-connector", "trino-connector-473-478", "build", "libs");
  }
}
