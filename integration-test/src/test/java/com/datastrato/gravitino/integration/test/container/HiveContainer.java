/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;

import com.datastrato.gravitino.catalog.hive.HiveClientPool;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class HiveContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_HIVE_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-hive";
  private static final int MYSQL_PORT = 3306;
  public static final int HDFS_DEFAULTFS_PORT = 9000;
  public static final int HIVE_METASTORE_PORT = 9083;

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
    int sleepTimeMillis = 10_000;
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
        LOG.info(
            "Hive container is not ready, recheck({}/{}) after {}ms",
            nRetry,
            retryLimit,
            sleepTimeMillis);
        Thread.sleep(sleepTimeMillis);
      } catch (RuntimeException e) {
        LOG.error(e.getMessage(), e);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    // Test hive client if it can connect to hive server
    boolean isHiveConnectSuccess = false;
    HiveConf hiveConf = new HiveConf();
    String hiveMetastoreUris =
        String.format("thrift://%s:%d", getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
    HiveClientPool hiveClientPool = new HiveClientPool(1, hiveConf);

    nRetry = 0;
    while (nRetry++ < retryLimit) {
      try {
        List<String> databases = hiveClientPool.run(IMetaStoreClient::getAllDatabases);
        if (!databases.isEmpty()) {
          isHiveConnectSuccess = true;
          break;
        }
        Thread.sleep(3000);
      } catch (TException | InterruptedException e) {
        LOG.warn("Failed to connect to hive server, retrying...", e);
      }
    }

    // Test HDFS client if it can connect to HDFS server
    boolean isHdfsConnectSuccess = false;
    nRetry = 0;
    Configuration conf = new Configuration();
    conf.set(
        "fs.defaultFS",
        String.format("hdfs://%s:%d", getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT));
    while (nRetry++ < retryLimit) {
      try (FileSystem fs = FileSystem.get(conf)) {
        Path directoryPath = new Path("/");
        FileStatus[] fileStatuses = fs.listStatus(directoryPath);
        if (fileStatuses.length > 0) {
          isHdfsConnectSuccess = true;
          break;
        }
        Thread.sleep(3000);
      } catch (IOException | InterruptedException e) {
        LOG.warn("Failed to connect to HDFS server, retrying...", e);
      }
    }

    LOG.info(
        "Hive container status: isHiveContainerReady={}, isHiveConnectSuccess={}, isHdfsConnectSuccess={}",
        isHiveContainerReady,
        isHiveConnectSuccess,
        isHdfsConnectSuccess);

    return isHiveContainerReady && isHiveConnectSuccess && isHdfsConnectSuccess;
  }

  public static class Builder extends BaseContainer.Builder<HiveContainer.Builder, HiveContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(MYSQL_PORT, HDFS_DEFAULTFS_PORT, HIVE_METASTORE_PORT);
    }

    @Override
    public HiveContainer build() {
      return new HiveContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
