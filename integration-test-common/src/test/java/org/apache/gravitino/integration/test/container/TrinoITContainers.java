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

import static org.apache.gravitino.integration.test.util.AbstractIT.ACTIVE_CI;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.DORIS;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.HIVE;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.HIVE_WITH_KERBEROS;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.HIVE_WITH_RANGER;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.KAFKA;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.MYSQL;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.POSTGRESQL;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.RANGER;
import static org.apache.gravitino.integration.test.util.AbstractIT.Service.TRINO;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.integration.test.util.CommandExecutor;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.ProcessData;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.shaded.com.google.common.collect.Maps;

public class TrinoITContainers implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoITContainers.class);

  public static String dockerComposeDir;

  //  private static ImmutableSet<String> servicesName =
  //      ImmutableSet.of("trino", "hive_metastore", "hdfs", "mysql", "postgresql");

  //  private static final String HiVE_SERVICE_NAME = "hive";
  //  private static final String HDFS_SERVICE_NAME = "hdfs";
  //  private static final String MYSQL_SERVICE_NAME = "mysql";
  //  private static final String POSTGRESQL_SERVICE_NAME = "postgresql";

  private Map<String, String> servicesUri = new HashMap<>();

  private Map<AbstractIT.Service, String> serviceIP = new HashMap<>();

  TrinoITContainers() {
    String dir = System.getenv("GRAVITINO_ROOT_DIR");
    if (Strings.isEmpty(dir)) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }

    dockerComposeDir = ITUtils.joinPath(dir, "integration-test-common", "docker-script");
  }

  public void launch(AbstractIT.Service... services) {
    launch(Maps.newHashMap(), services);
  }

  public void launchTrino(int gravitinoServerPort) {
    shutdown();

    Map<String, String> env = new HashMap<>();
    env.put("GRAVITINO_SERVER_PORT", String.valueOf(gravitinoServerPort));
    if (System.getProperty("gravitino.log.path") != null) {
      env.put("GRAVITINO_LOG_PATH", System.getProperty("gravitino.log.path"));
    }

    launch(env, TRINO, HIVE, MYSQL, POSTGRESQL);
  }

  public void launch(Map<String, String> env, AbstractIT.Service... services) {
    Set<String> serviceNames =
        Arrays.stream(services).map(s -> s.toString().toLowerCase()).collect(Collectors.toSet());

    String command =
        ITUtils.joinPath(dockerComposeDir, "launch.sh") + " " + String.join(" ", serviceNames);
    Object output =
        CommandExecutor.executeCommandLocalHost(
            command, false, ProcessData.TypesOfData.STREAMS_MERGED, env);
    LOG.info("Command {} output:\n{}", command, output);

    String outputString = output.toString();
    if (Strings.isNotEmpty(outputString)
        && !outputString.contains("All docker compose service is now available")) {
      throw new ContainerLaunchException("Failed to start containers:\n " + outputString);
    }

    resolveServerAddress(serviceNames);
  }

  //  public void launch(int gravitinoServerPort) throws Exception {
  //    shutdown();
  //
  //    Map<String, String> env = new HashMap<>();
  //    env.put("GRAVITINO_SERVER_PORT", String.valueOf(gravitinoServerPort));
  //    if (System.getProperty("gravitino.log.path") != null) {
  //      env.put("GRAVITINO_LOG_PATH", System.getProperty("gravitino.log.path"));
  //    }
  //
  //    String command = ITUtils.joinPath(dockerComposeDir, "launch.sh");
  //    Object output =
  //        CommandExecutor.executeCommandLocalHost(
  //            command, false, ProcessData.TypesOfData.STREAMS_MERGED, env);
  //    LOG.info("Command {} output:\n{}", command, output);
  //
  //    String outputString = output.toString();
  //    if (Strings.isNotEmpty(outputString)
  //        && !outputString.contains("All docker compose service is now available")) {
  //      throw new ContainerLaunchException("Failed to start containers:\n " + outputString);
  //    }
  //
  //    resolveServerAddress(servicesName);
  //  }

  private void resolveServerAddress(Set<String> serviceNames) {
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
          serviceIP.put(TRINO, address);
          servicesUri.put("trino", String.format("http://%s:8080", address));

        } else if (containerName.equals("hive")) {
          serviceIP.put(HIVE, address);
          servicesUri.put("hive", String.format("thrift://%s:9083", address));
          servicesUri.put("hdfs", String.format("hdfs://%s:9000", address));

        } else if (containerName.equals("hive-with-ranger")) {
          serviceIP.put(HIVE_WITH_RANGER, address);

        } else if (containerName.equals("hive-with-kerberos")) {
          serviceIP.put(HIVE_WITH_KERBEROS, address);

        } else if (containerName.equals("mysql")) {
          serviceIP.put(MYSQL, address);
          servicesUri.put("mysql", String.format("jdbc:mysql://%s:3306", address));

        } else if (containerName.equals("postgresql")) {
          serviceIP.put(POSTGRESQL, address);
          servicesUri.put("postgresql", String.format("jdbc:postgresql://%s", address));

        } else if (containerName.equals("kafka")) {
          serviceIP.put(KAFKA, address);
          servicesUri.put("kafka", String.format("%s:9092", address));
        } else if (containerName.equals("doris")) {
          serviceIP.put(DORIS, address);
          servicesUri.put("doris", String.format("jdbc:mysql://%s:9030", address));

        } else if (containerName.equals("ranger")) {
          serviceIP.put(RANGER, address);
          servicesUri.put("ranger", String.format("http://%s:6080", "localhost"));

        } else {
          throw new RuntimeException("Unexpected container name: " + containerName);
        }
      }
    } catch (Exception e) {
      throw new ContainerLaunchException("Unexpected container status :\n" + containerIpMapping, e);
    }

    for (String serviceName : serviceNames) {
      if (!servicesUri.containsKey(serviceName)
          && !serviceIP.containsKey(AbstractIT.Service.valueOf(serviceName.toUpperCase()))) {
        throw new ContainerLaunchException(
            String.format("The container for the %s service is not started: ", serviceName));
      }
    }
  }

  public void shutdown() {
    if ("true".equals(System.getenv(ACTIVE_CI))) {
      return;
    }

    String command = ITUtils.joinPath(dockerComposeDir, "shutdown.sh");
    Object output =
        CommandExecutor.executeCommandLocalHost(
            command, false, ProcessData.TypesOfData.STREAMS_MERGED);
    LOG.info("Command {} output:\n{}", command, output);
  }

  public String getServiceIpAddress(AbstractIT.Service service) {
    return serviceIP.get(service);
  }

  public String getTrinoUri() {
    return servicesUri.get("trino");
  }

  public String getHiveMetastoreUri() {
    return servicesUri.get("hive");
  }

  public String getHdfsUri() {
    return servicesUri.get("hdfs");
  }

  public String getHdfsIpAddress() {
    return serviceIP.get(HIVE);
  }

  public String getMysqlUri() {
    return servicesUri.get("mysql");
  }

  public String getPostgresqlUri() {
    return servicesUri.get("postgresql");
  }

  public String getKafkaUri() {
    return servicesUri.get("kafka");
  }

  public String getDorisUri() {
    return servicesUri.get("doris");
  }

  public String getRangerUri() {
    return servicesUri.get("ranger");
  }

  @Override
  public void close() throws Exception {
    shutdown();
  }
}
