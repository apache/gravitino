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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.integration.test.util.CommandExecutor;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.ProcessData;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class TrinoITContainers implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoITContainers.class);

  public static String dockerComposeDir;

  private static ImmutableSet<String> servicesName =
      ImmutableSet.of("trino", "hive_metastore", "hdfs", "mysql", "postgresql");

  Map<String, String> servicesUri = new HashMap<>();

  TrinoITContainers() {
    String dir = System.getenv("GRAVITINO_ROOT_DIR");
    if (Strings.isEmpty(dir)) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }

    dockerComposeDir = ITUtils.joinPath(dir, "integration-test-common", "docker-script");
  }

  public void launch(int gravitinoServerPort) throws Exception {
    launch(gravitinoServerPort, "hive2", false, 0);
  }

  public void launch(
      int gravitinoServerPort,
      String hiveRuntimeVersion,
      boolean isTrinoConnectorTest,
      int trinoWorkerNum)
      throws Exception {
    shutdown();

    Map<String, String> env = new HashMap<>();
    env.put("TRINO_WORKER_NUM", String.valueOf(trinoWorkerNum));
    env.put("GRAVITINO_SERVER_PORT", String.valueOf(gravitinoServerPort));
    env.put("HIVE_RUNTIME_VERSION", hiveRuntimeVersion);
    env.put("TRINO_CONNECTOR_TEST", String.valueOf(isTrinoConnectorTest));
    if (System.getProperty("gravitino.log.path") != null) {
      env.put("GRAVITINO_LOG_PATH", System.getProperty("gravitino.log.path"));
    }

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
            command, false, ProcessData.TypesOfData.STREAMS_MERGED);
    LOG.info("Command {} output:\n{}", command, output);

    // expect the output to be like:
    // trino:10.20.30.21
    // hive:10.20.30.19
    // mysql:10.20.30.20
    // postgresql:10.20.30.18

    String containerIpMapping = output.toString();
    if (containerIpMapping.isEmpty()) {
      throw new ContainerLaunchException(
          "Failed to get the container status, the containers have not started");
    }

    try {
      String[] containerInfos = containerIpMapping.split("\n");
      for (String container : containerInfos) {
        String[] info = container.split(":");

        String containerName = info[0];
        String address = info[1];

        if (containerName.equals("trino")) {
          servicesUri.put("trino", String.format("http://%s:8080", address));
        } else if (containerName.equals("hive")) {
          servicesUri.put("hive_metastore", String.format("thrift://%s:9083", address));
          servicesUri.put("hdfs", String.format("hdfs://%s:9000", address));
        } else if (containerName.equals("mysql")) {
          servicesUri.put("mysql", String.format("jdbc:mysql://%s:3306", address));
        } else if (containerName.equals("postgresql")) {
          servicesUri.put("postgresql", String.format("jdbc:postgresql://%s", address));
        }
      }
    } catch (Exception e) {
      throw new ContainerLaunchException("Unexpected container status :\n" + containerIpMapping, e);
    }

    for (String serviceName : servicesName) {
      if (!servicesUri.containsKey(serviceName)) {
        throw new ContainerLaunchException(
            String.format("The container for the %s service is not started: ", serviceName));
      }
    }
  }

  public void shutdown() {
    String command = ITUtils.joinPath(dockerComposeDir, "shutdown.sh");
    Object output =
        CommandExecutor.executeCommandLocalHost(
            command, false, ProcessData.TypesOfData.STREAMS_MERGED);
    LOG.info("Command {} output:\n{}", command, output);
  }

  public String getTrinoUri() {
    return servicesUri.get("trino");
  }

  public String getHiveMetastoreUri() {
    return servicesUri.get("hive_metastore");
  }

  public String getHdfsUri() {
    return servicesUri.get("hdfs");
  }

  public String getMysqlUri() {
    return servicesUri.get("mysql");
  }

  public String getPostgresqlUri() {
    return servicesUri.get("postgresql");
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }
}
