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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
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
    // Set here rather than in waitStrategy(), which runs before the constructor sets the auth.
    container.waitingFor(kerberosEnabled ? kerberosReadiness() : readiness());
    withStartupTimeout(startupTimeout());
    super.start();
  }

  // The port check's five minutes plus the 330 seconds the HTTP and the CLI checks each had before.
  @Override
  protected Duration startupTimeout() {
    return Duration.ofMinutes(5).plusSeconds(330);
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

  /**
   * Waits for HDFS through the NameNode's JMX, creates the warehouse directory through WebHDFS and
   * waits for the metastore and HiveServer2 ports. The Hive CLI checks this replaces each boot a
   * JVM with an embedded metastore, which costs several seconds per call; the metastore port covers
   * Hive itself.
   */
  private WaitStrategy readiness() {
    AtomicInteger namenodePort = new AtomicInteger();
    return new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(
            new ProbeWaitStrategy(
                    "HDFS to report a live DataNode",
                    target -> {
                      for (int port : NAMENODE_HTTP_PORTS) {
                        String jmx =
                            httpRequest(
                                "GET",
                                format(
                                    "http://%s:%d/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState",
                                    ipAddress(target.getContainerInfo()), port));
                        if (hasLiveDataNode(jmx)) {
                          namenodePort.set(port);
                          return true;
                        }
                      }
                      return false;
                    })
                .withPollInterval(Duration.ofMillis(500)))
        // `CREATE TABLE` through the Hive CLI used to create this directory as the Hadoop user;
        // tests that write to the default database expect it to exist.
        .withStrategy(
            new ProbeWaitStrategy(
                    "WebHDFS to create " + WAREHOUSE_DIR,
                    target -> {
                      String response =
                          httpRequest(
                              "PUT",
                              format(
                                  "http://%s:%d/webhdfs/v1%s?op=MKDIRS&user.name=%s",
                                  ipAddress(target.getContainerInfo()),
                                  namenodePort.get(),
                                  WAREHOUSE_DIR,
                                  getHadoopUser()));
                      return response != null && response.contains("true");
                    })
                .withPollInterval(Duration.ofMillis(500)))
        .withStrategy(portProbe(HIVE_METASTORE_PORT, "Hive Metastore"))
        // Tests run HiveQL over JDBC; Kerberos Hive tests still use the CLI.
        .withStrategy(portProbe(HIVE_SERVICE_PORT, "HiveServer2"));
  }

  /**
   * Waits for the image's check-status.sh and a {@code CREATE TABLE} through the Hive CLI, then for
   * the metastore port. Kerberos Hive keeps the CLI: over HTTP the NameNode wants SPNEGO, and the
   * CLI table creates the warehouse directory as the image's own Kerberos user, with the
   * permissions tests such as HiveUserAuthenticationIT rely on.
   */
  private WaitStrategy kerberosReadiness() {
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS default.employee ( eid int, name String, "
            + "salary String, destination String) ";
    return new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(
            new ProbeWaitStrategy(
                    "HDFS and the Hive CLI to pass check-status.sh",
                    target -> succeeds(target, "bash", "/tmp/check-status.sh"))
                // Each report runs two more CLIs, so only print it once Hive has run out of time.
                .withTimeoutReport(target -> outputHiveStatus()))
        .withStrategy(
            new ProbeWaitStrategy(
                "the Hive CLI to create default.employee",
                target -> succeeds(target, "hive", "-e", createTableSQL)))
        .withStrategy(portProbe(HIVE_METASTORE_PORT, "Hive Metastore"));
  }

  private static boolean succeeds(WaitStrategyTarget target, String... commandAndArgs)
      throws IOException, InterruptedException {
    Container.ExecResult result = target.execInContainer(commandAndArgs);
    if (result.getExitCode() != 0) {
      LOG.info(
          "Command [{}] exited with {}, stdout: {}, stderr: {}",
          String.join(" ", commandAndArgs),
          result.getExitCode(),
          result.getStdout(),
          result.getStderr());
      return false;
    }
    return true;
  }

  private static WaitStrategy portProbe(int port, String service) {
    return new ProbeWaitStrategy(
        format("%s to accept connections on port %d", service, port),
        target -> {
          try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ipAddress(target.getContainerInfo()), port), 3000);
            return true;
          }
        });
  }

  /**
   * Returns whether a NameNode {@code FSNamesystemState} JMX response reports a live DataNode.
   *
   * @param jmx the response body, or null when the request failed
   * @return true when at least one DataNode is live
   */
  static boolean hasLiveDataNode(@Nullable String jmx) {
    return jmx != null && jmx.matches("(?s).*\"NumLiveDataNodes\"\\s*:\\s*[1-9].*");
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
