/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class KerberosHDFSContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(KerberosHDFSContainer.class);

  public static final String DEFAULT_IMAGE =
      System.getenv("GRAVITINO_CI_KERBEROS_HDFS_DOCKER_IMAGE");
  public static final String HOST_NAME = "hdfs_host";

  private static final String HDFS_LOG_PATH = "/usr/local/hadoop/logs";
  public static final int HDFS_DEFAULTFS_PORT = 9000;
  public static final int KDC_PORT = 88;
  public static final int KDC_ADMIN_PORT = 749;

  public static KerberosHDFSContainer.Builder builder() {
    return new KerberosHDFSContainer.Builder();
  }

  protected KerberosHDFSContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "HDFSContainer")));
    withStartupTimeout(Duration.ofMinutes(5));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("HDFS container startup failed!", checkContainerStatus(5));
  }

  @Override
  public void close() {
    copyHDFSLog();
    super.close();
  }

  private void copyHDFSLog() {
    try {
      String destPath = System.getenv("IT_PROJECT_DIR");
      LOG.info("Copy HDFS log file to {}", destPath);

      String hdfsTarFileName = "/hdfs.tar";

      // Pack the jar files
      container.execInContainer("tar", "cf", hdfsTarFileName, HDFS_LOG_PATH);

      // Copy the jar files
      container.copyFileFromContainer(hdfsTarFileName, destPath + hdfsTarFileName);
    } catch (Exception e) {
      LOG.error("Failed to copy container log to local", e);
    }
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    await()
        .atMost(100, TimeUnit.SECONDS)
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
                  LOG.info("HDFS container startup success!");
                  return true;
                }
              } catch (RuntimeException e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            });

    return true;
  }

  public static class Builder
      extends BaseContainer.Builder<KerberosHDFSContainer.Builder, KerberosHDFSContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(HDFS_DEFAULTFS_PORT, KDC_PORT, KDC_ADMIN_PORT);
    }

    @Override
    public KerberosHDFSContainer build() {
      return new KerberosHDFSContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
