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

import static java.lang.String.format;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

public class HiveContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

  public static final String DEFAULT_IMAGE = TestImages.HIVE;
  public static final String KERBEROS_IMAGE = TestImages.KERBEROS_HIVE;

  public static final String HOST_NAME = "gravitino-ci-hive";
  public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  // Specify the Hive version to start the Hive container, currently support `hive2`(default) and
  // `hive3`
  public static final String HIVE_RUNTIME_VERSION = "HIVE_RUNTIME_VERSION";
  public static final String HIVE2 = "hive2"; // The Hive container default version
  public static final String HIVE3 = "hive3";
  public static final String ENABLE_SQL_BASE_AUTHORIZATION = "ENABLE_SQL_BASE_AUTHORIZATION";
  private static final int MYSQL_PORT = 3306;
  public static final int HDFS_DEFAULTFS_PORT = 9000;
  public static final int HIVE_METASTORE_PORT = 9083;
  public static final int HIVE_SERVICE_PORT = 10000;

  private static final String HIVE_LOG_PATH = "/tmp/root/";
  private static final String HDFS_LOG_PATH = "/usr/local/hadoop/logs/";
  // The image ships Hadoop 3 (NameNode web port 9870) for hive3 and Hadoop 2 (50070) for hive2.
  private static final int[] NAMENODE_HTTP_PORTS = {9870, 50070};
  private static final String WAREHOUSE_DIR = "/user/hive/warehouse";
  // The image's core-site.xml, which its start script copies into the Hadoop configuration.
  private static final String HADOOP_CONF_TO_COPY = "/tmp/hadoop-conf/core-site.xml";
  // The Hive images' entrypoint.
  private static final String IMAGE_START = "/usr/local/sbin/start.sh";
  // The image's core-site.xml already lets these users impersonate.
  private static final Set<String> USERS_ALLOWED_TO_IMPERSONATE =
      ImmutableSet.of("root", "hive", "gravitino");

  private final boolean kerberosEnabled;

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
      Optional<Network> network,
      boolean kerberosEnabled) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
    this.kerberosEnabled = kerberosEnabled;
  }

  public String getHostName() {
    return hostName;
  }

  /**
   * Returns the user the container's Hadoop and Hive processes run as: {@code HADOOP_USER_NAME}
   * when the test sets it, else root. Clients that write to HDFS should act as this user so they
   * get the same ownership as the old {@code docker exec hive} calls.
   */
  public String getHadoopUser() {
    return container.getEnvMap().getOrDefault(HADOOP_USER_NAME, "root");
  }

  // HDFS has no setting to skip syncing, so its NameNode and DataNode directories go on tmpfs: both
  // run as root with the default hadoop.tmp.dir, /tmp/hadoop-root. Not all of /tmp, which holds the
  // image's check-status.sh. The metastore MySQL keeps its data on disk and skips syncing instead.
  @Override
  protected void applyDisposableData(GenericContainer<?> container) {
    container
        .withTmpFs(Map.of("/tmp/hadoop-root", DISPOSABLE_DATA_TMPFS))
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("disposable/hive-metastore-mysql.cnf"),
            "/etc/mysql/conf.d/zz-gravitino-disposable.cnf");
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "HiveContainer-" + hostName)));
  }

  @Override
  public void start() {
    if (!kerberosEnabled) {
      allowHadoopUserToImpersonate();
    }
    super.start();
    Preconditions.check("Hive container startup failed!", checkContainerStatus(15));
  }

  /**
   * Lets the Hadoop user, which every process in the container runs as, impersonate users.
   *
   * <p>HiveServer2 runs each JDBC statement as the session user (doAs), so it impersonates even
   * when that user is itself; the image only allows that for {@code hive}, {@code root} and {@code
   * gravitino}. The rule goes into the configuration the launch script copies in, before the
   * NameNode starts.
   */
  private void allowHadoopUserToImpersonate() {
    String user = getHadoopUser();
    if (USERS_ALLOWED_TO_IMPERSONATE.contains(user)) {
      return;
    }
    String rule =
        format(
            "<property><name>hadoop.proxyuser.%1$s.hosts</name><value>*</value></property>"
                + "<property><name>hadoop.proxyuser.%1$s.groups</name><value>*</value></property>",
            user);
    // Fail the start, rather than let JDBC sessions fail later, if a new image changes the file.
    String script =
        format(
            "sed -i 's|</configuration>|%1$s</configuration>|' %2$s"
                + " && grep -q 'hadoop.proxyuser.%3$s.hosts' %2$s"
                + " || { echo 'Cannot add the proxy-user rule to %2$s' >&2; exit 1; }",
            rule, HADOOP_CONF_TO_COPY, user);
    container.withCreateContainerCmdModifier(
        cmd -> cmd.withEntrypoint("/bin/bash", "-c", script + " && exec /bin/bash " + IMAGE_START));
  }

  @Override
  public void close() {
    // A private container is closed by its JUnit store and again by the run's closer at the end of
    // the
    // run; only a running container has logs to copy.
    if (container.isRunning()) {
      copyHiveLog();
    }
    super.close();
  }

  private void copyHiveLog() {
    try {
      String destPath = System.getenv("IT_PROJECT_DIR");
      LOG.info("Copy Hive log file to {}", destPath);

      String hiveLogJarPath = "/hive.tar";
      String HdfsLogJarPath = "/hdfs.tar";

      // Pack the jar files
      container.execInContainer("tar", "cf", hiveLogJarPath, HIVE_LOG_PATH);
      container.execInContainer("tar", "cf", HdfsLogJarPath, HDFS_LOG_PATH);

      container.copyFileFromContainer(hiveLogJarPath, destPath + File.separator + "hive.tar");
      container.copyFileFromContainer(HdfsLogJarPath, destPath + File.separator + "hdfs.tar");
    } catch (Exception e) {
      LOG.warn("Can't copy Hive log for:", e);
    }
  }

  private void outputHiveStatus() {
    Container.ExecResult execResult = executeInContainer("hdfs", "dfsadmin", "-report");
    LOG.info("HDFS report, stdout: {}, stderr: {}", execResult.getStdout(), execResult.getStderr());

    execResult = executeInContainer("hive", "-e", "\"select 1;\"");
    LOG.info("Hive report, stdout: {}, stderr: {}", execResult.getStdout(), execResult.getStderr());
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    if (kerberosEnabled) {
      awaitReadyWithHiveCli();
      awaitPort(HIVE_METASTORE_PORT, "Hive Metastore");
    } else {
      awaitReadyOverHttp();
      awaitPort(HIVE_METASTORE_PORT, "Hive Metastore");
      // Tests run HiveQL over JDBC; Kerberos Hive tests still use the CLI.
      awaitPort(HIVE_SERVICE_PORT, "HiveServer2");
    }
    return true;
  }

  /**
   * Waits for HDFS through the NameNode's JMX and creates the warehouse directory through WebHDFS.
   * The Hive CLI checks this replaces each boot a JVM with an embedded metastore, which costs
   * several seconds per call; the metastore port check that follows covers Hive itself.
   */
  private void awaitReadyOverHttp() {
    String containerIp = getContainerIpAddress();
    int[] namenodePort = new int[1];
    await()
        .atMost(150, TimeUnit.SECONDS)
        .pollDelay(Duration.ZERO)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              for (int port : NAMENODE_HTTP_PORTS) {
                String jmx =
                    httpRequest(
                        "GET",
                        format(
                            "http://%s:%d/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState",
                            containerIp, port));
                if (jmx != null && jmx.matches("(?s).*\"NumLiveDataNodes\"\\s*:\\s*[1-9].*")) {
                  namenodePort[0] = port;
                  return true;
                }
              }
              return false;
            });

    // `CREATE TABLE` through the Hive CLI used to create this directory as the Hadoop user; tests
    // that write to the default database expect it to exist.
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollDelay(Duration.ZERO)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              String response =
                  httpRequest(
                      "PUT",
                      format(
                          "http://%s:%d/webhdfs/v1%s?op=MKDIRS&user.name=%s",
                          containerIp, namenodePort[0], WAREHOUSE_DIR, getHadoopUser()));
              return response != null && response.contains("true");
            });
    LOG.info("Hive container startup success!");
  }

  /** Returns the response body, or null when the request fails or returns a non-2xx status. */
  private static String httpRequest(String method, String url) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod(method);
      connection.setConnectTimeout(2000);
      connection.setReadTimeout(5000);
      int status = connection.getResponseCode();
      if (status / 100 != 2) {
        String error = "";
        if (connection.getErrorStream() != null) {
          try (InputStream body = connection.getErrorStream()) {
            error = new String(body.readAllBytes(), StandardCharsets.UTF_8);
          }
        }
        LOG.debug("{} {} returned HTTP {}: {}", method, url, status, error);
        return null;
      }
      try (InputStream body = connection.getInputStream()) {
        return new String(body.readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      LOG.debug("{} {} failed: {}", method, url, e.toString());
      return null;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  // Ready means HDFS has a live data node and the Hive CLI can list databases.
  private void awaitHiveCli() {
    await()
        .atMost(150, TimeUnit.SECONDS)
        .pollDelay(Duration.ZERO)
        // Each check runs the Hive CLI, which takes seconds; a short interval only stops it from
        // waiting long after Hive is up.
        .pollInterval(1, TimeUnit.SECONDS)
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
                  LOG.info("{}, stderr: {}", message, execResult.getStderr());
                } else {
                  LOG.info("Hive container startup success!");
                  return true;
                }
              } catch (RuntimeException e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            });
  }

  private void awaitReadyWithHiveCli() {
    try {
      awaitHiveCli();
    } catch (RuntimeException e) {
      // Each report runs two more CLIs, so only print it once Hive has run out of time.
      outputHiveStatus();
      throw e;
    }

    // Creating a table also creates /user/hive/warehouse in HDFS as the image's own user; Kerberos
    // tests such as HiveUserAuthenticationIT rely on that directory and its permissions.
    final String createTableSQL =
        "CREATE TABLE IF NOT EXISTS default.employee ( eid int, name String, "
            + "salary String, destination String) ";
    await()
        .atMost(120, TimeUnit.SECONDS)
        .pollDelay(Duration.ZERO)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result = executeInContainer("hive", "-e", createTableSQL);
                if (result.getExitCode() == 0) {
                  return true;
                }

                LOG.warn(
                    "Failed to execute sql: {}, Std-out: {}, Std-error:{}",
                    createTableSQL,
                    result.getStdout(),
                    result.getStderr());
              } catch (Exception e) {
                LOG.error("Failed to execute sql: {}", createTableSQL, e);
              }
              return false;
            });
  }

  private void awaitPort(int port, String service) {
    String containerIp = getContainerIpAddress();
    // The metastore opens its port only after its schema is set up; under CI load that can take
    // well over 10 seconds, so give it the same kind of budget as the other stages.
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollDelay(Duration.ZERO)
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(containerIp, port), 3000);
                return true;
              } catch (Exception e) {
                LOG.warn("Can't connect to {}:[{}:{}]", service, containerIp, port, e);
              }
              return false;
            });
  }

  public static class Builder extends BaseContainer.Builder<Builder, HiveContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts =
          ImmutableSet.of(MYSQL_PORT, HDFS_DEFAULTFS_PORT, HIVE_METASTORE_PORT, HIVE_SERVICE_PORT);
    }

    private String generateImageName() {
      String hiveDockerImageName = image;
      if (kerberosEnabled) {
        hiveDockerImageName = KERBEROS_IMAGE;
      }
      return hiveDockerImageName;
    }

    @Override
    public HiveContainer build() {
      return new HiveContainer(
          generateImageName(),
          hostName,
          exposePorts,
          extraHosts,
          filesToMount,
          envVars,
          network,
          kerberosEnabled);
    }
  }
}
