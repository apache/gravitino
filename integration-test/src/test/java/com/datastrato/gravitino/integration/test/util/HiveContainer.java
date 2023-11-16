/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class HiveContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

  public static final String DEFAULT_IMAGE = "datastrato/gravitino-ci-hive:0.1.5";
  public static final String HOST_NAME = "gravitino-ci-hive";
  private static final int MYSQL_PORT = 3306;
  private static final int YARN_SERVICE_PORT = 8088;
  private static final int HDFS_DEFAULTFS_PORT = 9000;
  public static final int HIVE_METASTORE_PORT = 9083;
  private static final int HIVESERVER2_PORT = 10000;
  private static final int HIVESERVER2_HTTP_PORT = 10002;
  private static final int HDFS_NAMENODE_PORT = 50070;
  private static final int HDFS_DATANODE_HTTP_SERVER_PORT = 50075;
  private static final int HDFS_DATANODE_DATA_TRANSFER_PORT = 50010;

  public static Builder builder() {
    return new Builder();
  }

  protected HiveContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "HiveContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("Hive container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isHiveContainerReady = false;
    while (nRetry++ < retryLimit) {
      try {
        String[] commandAndArgs = new String[] {"bash", "/tmp/check-status.sh"};
        Container.ExecResult execResult = executeInContainer(commandAndArgs);
        if (execResult.getExitCode() != 0) {
          String message =
              format(
                  "Command [%s] exited with %s",
                  String.join(" ", commandAndArgs), execResult.getExitCode());
          LOG.error("{}", message);
          LOG.error("stderr: {}", execResult.getStderr());
          LOG.error("stdout: {}", execResult.getStdout());
        } else {
          LOG.info("Hive container startup success!");
          isHiveContainerReady = true;
          break;
        }
        Thread.sleep(5000);
      } catch (RuntimeException e) {
        LOG.error(e.getMessage(), e);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return isHiveContainerReady;
  }

  public static class Builder extends BaseContainer.Builder<HiveContainer.Builder, HiveContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts =
          ImmutableSet.of(
              MYSQL_PORT,
              YARN_SERVICE_PORT,
              HDFS_DEFAULTFS_PORT,
              HIVE_METASTORE_PORT,
              HIVESERVER2_PORT,
              HIVESERVER2_HTTP_PORT,
              HDFS_NAMENODE_PORT,
              HDFS_DATANODE_HTTP_SERVER_PORT,
              HDFS_DATANODE_DATA_TRANSFER_PORT);
    }

    @Override
    public HiveContainer build() {
      return new HiveContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
