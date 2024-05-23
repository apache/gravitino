/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class HiveContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_HIVE_DOCKER_IMAGE");
  public static final String KERBEROS_IMAGE =
      System.getenv("GRAVITINO_CI_KERBEROS_HIVE_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-hive";
  private static final int MYSQL_PORT = 3306;
  public static final int HDFS_DEFAULTFS_PORT = 9000;
  public static final int HIVE_METASTORE_PORT = 9083;

  private static final String HIVE_LOG_PATH = "/tmp/root/";
  private static final String HDFS_LOG_PATH = "/usr/local/hadoop/logs/";

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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "HiveContainer-" + hostName)));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("Hive container startup failed!", checkContainerStatus(15));
  }

  @Override
  public void close() {
    copyHiveLog();
    super.close();
  }

  private void copyHiveLog() {
    try {
      String destPath = System.getenv("IT_PROJECT_DIR");
      LOG.info("Copy hive log file to {}", destPath);

      String hiveLogJarPath = "/hive.tar";
      String HdfsLogJarPath = "/hdfs.tar";

      // Pack the jar files
      container.execInContainer("tar", "cf", hiveLogJarPath, HIVE_LOG_PATH);
      container.execInContainer("tar", "cf", HdfsLogJarPath, HDFS_LOG_PATH);

      container.copyFileFromContainer(hiveLogJarPath, destPath + File.separator + "hive.tar");
      container.copyFileFromContainer(HdfsLogJarPath, destPath + File.separator + "hdfs.tar");
    } catch (Exception e) {
      LOG.warn("Can't copy hive log for:", e);
    }
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    await()
        .atMost(150, TimeUnit.SECONDS)
        .pollInterval(100 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
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
                  return true;
                }
              } catch (RuntimeException e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            });

    final String showDatabaseSQL = "show databases";
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(30 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result = executeInContainer("hive", "-e", showDatabaseSQL);
                if (result.getStdout().contains("default")) {
                  return true;
                }
              } catch (Exception e) {
                LOG.error("Failed to execute sql: {}", showDatabaseSQL, e);
              }
              return false;
            });
    final String createTableSQL =
        "CREATE TABLE IF NOT EXISTS default.employee ( eid int, name String, "
            + "salary String, destination String) ";
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(30 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result = executeInContainer("hive", "-e", createTableSQL);
                if (result.getExitCode() == 0) {
                  return true;
                }
              } catch (Exception e) {
                LOG.error("Failed to execute sql: {}", createTableSQL, e);
              }
              return false;
            });

    String containerIp = getContainerIpAddress();
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Socket socket = new Socket()) {
                socket.connect(
                    new InetSocketAddress(containerIp, HiveContainer.HIVE_METASTORE_PORT), 3000);
                return true;
              } catch (Exception e) {
                LOG.warn(
                    "Can't connect to Hive Metastore:[{}:{}]",
                    containerIp,
                    HiveContainer.HIVE_METASTORE_PORT,
                    e);
              }
              return false;
            });

    return true;
  }

  public static class Builder extends BaseContainer.Builder<Builder, HiveContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(MYSQL_PORT, HDFS_DEFAULTFS_PORT, HIVE_METASTORE_PORT);
    }

    @Override
    public HiveContainer build() {
      return new HiveContainer(
          kerberosEnabled ? KERBEROS_IMAGE : image,
          kerberosEnabled ? "kerberos-" + hostName : hostName,
          exposePorts,
          extraHosts,
          filesToMount,
          envVars,
          network);
    }
  }
}
