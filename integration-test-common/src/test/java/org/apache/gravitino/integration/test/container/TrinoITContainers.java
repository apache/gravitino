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
package org.apache.gravitino.integration.test.container;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.integration.test.util.CommandExecutor;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.ProcessData;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;

public class TrinoITContainers implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoITContainers.class);

  private static final String TEST_CONTAINERS = "TEST_CONTAINERS";

  public static String dockerComposeDir;

  Map<String, String> servicesUri = new HashMap<>();
  private final Map<String, String> containerEnv = new HashMap<>();

  TrinoITContainers() {
    String dir = System.getenv("GRAVITINO_ROOT_DIR");
    if (Strings.isEmpty(dir)) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }

    dockerComposeDir = ITUtils.joinPath(dir, "integration-test-common", "docker-script");
  }

  public void launch(int gravitinoServerPort) throws Exception {
    launch(gravitinoServerPort, "hive2", false, null, null, null, null);
  }

  public void launch(
      int gravitinoServerPort,
      String hiveRuntimeVersion,
      boolean isTrinoConnectorTest,
      Integer trinoWorkerNum,
      Integer trinoVersion,
      String trinoConnectorDir,
      String testContainers)
      throws Exception {
    servicesUri.clear();

    Map<String, String> env = new HashMap<>();
    if (trinoWorkerNum != null) {
      env.put("TRINO_WORKER_NUM", String.valueOf(trinoWorkerNum));
    }
    if (trinoVersion != null) {
      env.put("TRINO_VERSION", String.valueOf(trinoVersion));
    }
    if (trinoConnectorDir != null) {
      File dir = new File(trinoConnectorDir);
      if (!dir.exists() || !dir.isDirectory() || dir.list().length == 0) {
        throw new Exception(
            "Gravitino trino connector directory %s does not exist or is empty"
                .formatted(trinoConnectorDir));
      }
      env.put("GRAVITINO_TRINO_CONNECTOR_DIR", trinoConnectorDir);
    }
    env.put("GRAVITINO_SERVER_PORT", String.valueOf(gravitinoServerPort));
    env.put("TRINO_CONNECTOR_TEST", String.valueOf(isTrinoConnectorTest));
    if (hiveRuntimeVersion != null) {
      env.put("HIVE_RUNTIME_VERSION", hiveRuntimeVersion);
    }
    if (Strings.isNotBlank(testContainers)) {
      env.put(TEST_CONTAINERS, testContainers);
    }
    if (System.getProperty("gravitino.log.path") != null) {
      env.put("GRAVITINO_LOG_PATH", System.getProperty("gravitino.log.path"));
    }
    containerEnv.clear();
    containerEnv.putAll(env);

    shutdown();

    LOG.info("Launching containers with env: {}", env);
    String command = ITUtils.joinPath(dockerComposeDir, "launch.sh");
    Object output =
        CommandExecutor.executeCommandLocalHost(
            command, false, ProcessData.TypesOfData.STREAMS_MERGED, env);
    LOG.info("Command {} output:\n{}", command, output);

    String outputString = output.toString();
    if (Strings.isNotEmpty(outputString)
        && !outputString.contains("All docker compose service is now available")) {
      throw new ContainerLaunchException("Failed to start containers:\n " + outputString);
    }

    resolveServerAddress();
  }

  private void resolveServerAddress() throws Exception {
    String command = ITUtils.joinPath(dockerComposeDir, "inspect_ip.sh");
    Object output =
        CommandExecutor.executeCommandLocalHost(
            command, false, ProcessData.TypesOfData.STREAMS_MERGED, containerEnv);
    LOG.info("Command {} output:\n{}", command, output);

    // expect the output to be like:
    // trino_uri=http://10.20.30.21:8080
    // hive_uri=thrift://10.20.30.19:9083
    // hdfs_uri=hdfs://10.20.30.19:9000
    // mysql_uri=jdbc:mysql://10.20.30.20:3306
    // postgresql_uri=jdbc:postgresql://10.20.30.18

    String serviceURLs = output.toString();
    if (serviceURLs.isEmpty()) {
      throw new ContainerLaunchException(
          "Failed to get the container status, the containers have not started");
    }

    try {
      String[] serviceInfos = serviceURLs.split("\n");
      for (String serviceInfo : serviceInfos) {
        String[] info = serviceInfo.split("=", 2);
        if (info.length != 2) {
          continue;
        }

        servicesUri.put(info[0], info[1]);
      }
    } catch (Exception e) {
      throw new ContainerLaunchException("Unexpected container status :\n" + serviceURLs, e);
    }

    if (!servicesUri.containsKey("trino_uri")) {
      throw new ContainerLaunchException("The container for the trino service is not started");
    }
  }

  public void shutdown() {
    String command = ITUtils.joinPath(dockerComposeDir, "shutdown.sh");
    Object output =
        CommandExecutor.executeCommandLocalHost(
            command, false, ProcessData.TypesOfData.STREAMS_MERGED, containerEnv);
    LOG.info("Command {} output:\n{}", command, output);
  }

  public Map<String, String> getServiceUrls() {
    return servicesUri;
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }
}
