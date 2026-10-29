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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.KerberosHdfs;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.builder.Transferable;
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

  /** The realm of the Kerberos image's KDC. */
  public static final String KERBEROS_REALM = "HADOOPKRB";
  // The Kerberos image's second metastore, from its hive-site1.xml.
  private static final int SECOND_HIVE_METASTORE_PORT = 19083;

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
  // Where the container gets the script that creates a test's Kerberos principals.
  static final String PRINCIPALS_SCRIPT = "/tmp/gravitino-kerberos-principals.sh";
  // The Kerberos image's start script logs in as hive/<host> right after creating its own
  // principals and before it configures and starts HDFS; the test's principals go in before it.
  static final String PRINCIPALS_HOOK_ANCHOR = "kinit hive/";

  private final boolean kerberosEnabled;
  private final List<String> kerberosPrincipals;
  // The NameNode and DataNode ports when HDFS is host-reachable (Builder#withHostReachableHdfs);
  // 0 when the image's defaults stay.
  private final int namenodePort;
  private final int datanodePort;

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
      boolean kerberosEnabled,
      List<String> kerberosPrincipals) {
    this(
        image,
        hostName,
        ports,
        extraHosts,
        filesToMount,
        envVars,
        network,
        kerberosEnabled,
        kerberosPrincipals,
        0,
        0);
  }

  private HiveContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network,
      boolean kerberosEnabled,
      List<String> kerberosPrincipals,
      int namenodePort,
      int datanodePort) {
    super(
        withFixedPorts(image, namenodePort, datanodePort),
        hostName,
        ports,
        extraHosts,
        filesToMount,
        envVars,
        network);
    this.namenodePort = namenodePort;
    this.datanodePort = datanodePort;
    this.kerberosEnabled = kerberosEnabled;
    this.kerberosPrincipals = List.copyOf(kerberosPrincipals);
    if (!kerberosEnabled && !this.kerberosPrincipals.isEmpty()) {
      throw new IllegalArgumentException(
          "Kerberos principals need a Kerberos Hive: " + this.kerberosPrincipals);
    }
  }

  /**
   * Returns where the Kerberos Hive keeps the keytab of a principal from {@link
   * HiveFixture#kerberosPrincipals()}. Tests copy it out with {@code copyFileFromContainer}.
   *
   * @param principal the principal's name, without realm
   * @return the keytab's path in the container
   */
  public static String kerberosKeytab(String principal) {
    return "/" + principal + ".keytab";
  }

  // Wraps a Hive that runs outside this JVM's Testcontainers; see ExternalEndpoints.
  protected HiveContainer(TestEndpoints.Hive endpoints) {
    super(endpoints, HOST_NAME);
    this.kerberosEnabled = false;
    this.kerberosPrincipals = List.of();
    this.namenodePort = 0;
    this.datanodePort = 0;
  }

  // The same port inside and outside the container, because HDFS URIs carry it: the metastore
  // creates table directories through the URI a test sends, and hands URIs back.
  private static GenericContainer<?> withFixedPorts(
      String image, int namenodePort, int datanodePort) {
    if (namenodePort == 0) {
      return new GenericContainer<>(image);
    }
    return new FixedPortContainer(image, namenodePort, datanodePort);
  }

  // GenericContainer only lets a subclass bind a fixed host port.
  private static final class FixedPortContainer extends GenericContainer<FixedPortContainer> {
    FixedPortContainer(String image, int... ports) {
      super(image);
      for (int port : ports) {
        addFixedExposedPort(port, port);
      }
    }
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot pick a free port", e);
    }
  }

  public String getHostName() {
    return hostName;
  }

  /**
   * Returns the metastore address the test JVM connects to: the Docker host and mapped port for a
   * managed container, or {@code gravitino.test.hive.metastore-uri} for an external one.
   *
   * @return {@code thrift://host:port}
   */
  public String metastoreUri() {
    return isExternal()
        ? endpoints(TestEndpoints.Hive.class).metastoreUri()
        : format("thrift://%s:%d", getHost(), getMappedPort(HIVE_METASTORE_PORT));
  }

  /**
   * Returns the HDFS address the test JVM connects to: the Docker host and mapped NameNode port for
   * a managed container, or {@code gravitino.test.hive.hdfs-uri} for an external one. Clients must
   * also apply {@link #hdfsClientConf()}.
   *
   * @return {@code hdfs://host:port}
   */
  public String hdfsUri() {
    if (isExternal()) {
      return endpoints(TestEndpoints.Hive.class).hdfsUri();
    }
    return namenodePort > 0
        ? format("hdfs://%s:%d", getHost(), namenodePort)
        : format("hdfs://%s:%d", getHost(), getMappedPort(HDFS_DEFAULTFS_PORT));
  }

  /**
   * Returns the Hadoop settings an HDFS client in the test JVM needs besides {@code fs.defaultFS}.
   * The NameNode hands out DataNode addresses as the DataNode registered them, so a client outside
   * the container's network must be told how to reach them: {@code
   * gravitino.test.hive.hdfs-client-conf} ({@code k=v,k=v}) for an external container, such as
   * {@code dfs.client.use.datanode.hostname=true}; nothing for a managed one, whose DataNode the
   * Docker host reaches by its registered address.
   *
   * @return the settings
   */
  public Map<String, String> hdfsClientConf() {
    if (isExternal()) {
      return endpoints(TestEndpoints.Hive.class).hdfsClientConf();
    }
    // Host-reachable HDFS registers its DataNode as "localhost"; by name, the Docker host's mapped
    // port serves it, where the DataNode's container IP would not on Docker Desktop.
    return namenodePort > 0 ? Map.of("dfs.client.use.datanode.hostname", "true") : Map.of();
  }

  /**
   * Returns a Hadoop configuration for HDFS in the test JVM: {@link #hdfsUri()} as the default file
   * system plus {@link #hdfsClientConf()}.
   *
   * @return the configuration
   */
  public Configuration hdfsConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsUri());
    hdfsClientConf().forEach(conf::set);
    return conf;
  }

  /**
   * Returns the user the container's Hadoop and Hive processes run as: {@code HADOOP_USER_NAME}
   * when the test sets it, else root. Clients that write to HDFS should act as this user so they
   * get the same ownership as the old {@code docker exec hive} calls.
   */
  public String getHadoopUser() {
    return isExternal()
        ? Optional.ofNullable(endpoints(TestEndpoints.Hive.class).hadoopUser()).orElse("root")
        : container.getEnvMap().getOrDefault(HADOOP_USER_NAME, "root");
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
    if (isExternal()) {
      super.start();
      return;
    }
    if (!kerberosEnabled) {
      String script = allowHadoopUserToImpersonate() + hostReachableHdfs();
      if (!script.isEmpty()) {
        container.withCreateContainerCmdModifier(
            cmd -> cmd.withEntrypoint("/bin/bash", "-c", script + "exec /bin/bash " + IMAGE_START));
      }
    } else if (!kerberosPrincipals.isEmpty()) {
      createKerberosPrincipals();
    }
    // Set here rather than in waitStrategy(), which runs before the constructor sets the auth.
    container.waitingFor(kerberosEnabled ? kerberosReadiness() : readiness(getHadoopUser()));
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
  private String allowHadoopUserToImpersonate() {
    String user = getHadoopUser();
    if (USERS_ALLOWED_TO_IMPERSONATE.contains(user)) {
      return "";
    }
    String rule =
        format(
            "<property><name>hadoop.proxyuser.%1$s.hosts</name><value>*</value></property>"
                + "<property><name>hadoop.proxyuser.%1$s.groups</name><value>*</value></property>",
            user);
    // Fail the start, rather than let JDBC sessions fail later, if a new image changes the file.
    return format(
        "{ sed -i 's|</configuration>|%1$s</configuration>|' %2$s"
            + " && grep -q 'hadoop.proxyuser.%3$s.hosts' %2$s; }"
            + " || { echo 'Cannot add the proxy-user rule to %2$s' >&2; exit 1; }; ",
        rule, HADOOP_CONF_TO_COPY, user);
  }

  /**
   * Moves the NameNode and DataNode to the chosen ports and lets them serve a host name of {@code
   * localhost}: the daemons bind every interface instead of the loopback that name resolves to, and
   * the NameNode accepts a DataNode whose name does not resolve to its IP. The image's launch
   * script copies these files into the Hadoop and Hive configuration and substitutes the host name.
   */
  private String hostReachableHdfs() {
    if (namenodePort == 0) {
      return "";
    }
    String hdfsSite = "/tmp/hadoop-conf/hdfs-site.xml";
    String bindAll =
        "<property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>"
            + "<property><name>dfs.namenode.datanode.registration.ip-hostname-check</name>"
            + "<value>false</value></property>";
    return format(
        "sed -i 's|:%1$d|:%2$d|g' /tmp/hadoop-conf/core-site.xml /tmp/hive-conf/hive-site.xml"
            + " /tmp/hive-conf/hive-site-for-sql-base-auth.xml"
            + " && sed -i 's|__REPLACE__HOST_NAME:50010|0.0.0.0:%3$d|' %4$s"
            + " && sed -i 's|</configuration>|%5$s</configuration>|' %4$s"
            + " && grep -q 'rpc-bind-host' %4$s"
            + " || { echo 'Cannot move HDFS to ports %2$d and %3$d' >&2; exit 1; }; ",
        HDFS_DEFAULTFS_PORT, namenodePort, datanodePort, hdfsSite, bindAll);
  }

  /**
   * Creates the test's principals and their keytabs while the container starts.
   *
   * <p>The KDC's database only exists once the image's start script has created it, so the
   * entrypoint hooks a script into the start script, next to the image's own principals and before
   * HDFS starts; readiness therefore implies the keytabs exist. A missing hook point or a failed
   * principal stops the start script, and with it the container.
   */
  private void createKerberosPrincipals() {
    container
        .withCopyToContainer(
            Transferable.of(principalsScript(kerberosPrincipals), 0755), PRINCIPALS_SCRIPT)
        .withCreateContainerCmdModifier(
            cmd -> cmd.withEntrypoint("/bin/bash", "-c", principalsEntrypoint(IMAGE_START)));
  }

  /**
   * Returns the script that creates each principal in {@link #KERBEROS_REALM} with a keytab at
   * {@link #kerberosKeytab(String)}, and fails if a keytab does not hold its principal. {@code
   * kadmin.local -q} exits 0 even when its query fails, so the keytab is the check.
   */
  static String principalsScript(List<String> principals) {
    StringBuilder script = new StringBuilder("set -e\n");
    for (String name : principals) {
      String principal = name + "@" + KERBEROS_REALM;
      String keytab = kerberosKeytab(name);
      script
          .append(format("kadmin.local -q 'addprinc -randkey %s'\n", principal))
          .append(format("kadmin.local -q 'xst -norandkey -k %s %s'\n", keytab, principal))
          .append(
              format(
                  "klist -k %1$s | grep -q ' %2$s$'"
                      + " || { echo 'No keytab for %2$s at %1$s' >&2; exit 1; }\n",
                  keytab, principal));
    }
    return script.toString();
  }

  /**
   * Returns the entrypoint that runs {@link #PRINCIPALS_SCRIPT} inside {@code imageStart}, before
   * the line with {@link #PRINCIPALS_HOOK_ANCHOR}, and then runs {@code imageStart}. It fails
   * without starting anything if that line is missing.
   */
  static String principalsEntrypoint(String imageStart) {
    String hook =
        format(
            "bash %s || { echo \"Cannot create the test Kerberos principals\" >&2; exit 1; }",
            PRINCIPALS_SCRIPT);
    return format(
        "sed -i '/%1$s/i %2$s' %3$s"
            + " && grep -q '^bash %4$s ' %3$s"
            + " || { echo 'Cannot hook the Kerberos principals into %3$s' >&2; exit 1; }"
            + " && exec /bin/bash %3$s",
        PRINCIPALS_HOOK_ANCHOR.replace("/", "\\/"), hook, imageStart, PRINCIPALS_SCRIPT);
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
    String destPath = System.getenv("IT_PROJECT_DIR");
    if (destPath == null || destPath.isEmpty()) {
      return;
    }
    LOG.info("Copy Hive log file to {}", destPath);
    try {
      ContainerLogs.copyArchive(
          container,
          Paths.get(destPath, ContainerLogs.fileName("hive.tar", container)),
          HIVE_LOG_PATH);
      ContainerLogs.copyArchive(
          container,
          Paths.get(destPath, ContainerLogs.fileName("hdfs.tar", container)),
          HDFS_LOG_PATH);
    } catch (Exception e) {
      LOG.warn("Can't copy Hive log for:", e);
    }
  }

  /**
   * Waits for HDFS through the NameNode's JMX, creates the warehouse directory through WebHDFS and
   * waits for the metastore and HiveServer2 ports. The Hive CLI checks this replaces each boot a
   * JVM with an embedded metastore, which costs several seconds per call; the metastore port covers
   * Hive itself. Any non-Kerberos container of the Hive image can wait with it, such as the Hive of
   * a {@link TrinoCluster}.
   *
   * @param hadoopUser the user that creates the warehouse directory, as the Hive CLI would have
   * @return the wait strategy
   */
  static WaitStrategy readiness(String hadoopUser) {
    AtomicInteger namenodePort = new AtomicInteger();
    return new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(liveDataNode(namenodePort))
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
                                  hadoopUser));
                      return response != null && response.contains("true");
                    })
                .withPollInterval(Duration.ofMillis(500)))
        .withStrategy(portProbe(HIVE_METASTORE_PORT, "Hive Metastore"))
        // Tests run HiveQL over JDBC.
        .withStrategy(portProbe(HIVE_SERVICE_PORT, "HiveServer2"));
  }

  /**
   * Waits for HDFS through the NameNode's JMX, creates the warehouse directory through a Kerberos
   * HDFS client and waits for both metastores' ports.
   *
   * <p>The image leaves the NameNode's web server on simple authentication (no {@code
   * hadoop.http.authentication.type} or filter initializer in its core-site.xml); only WebHDFS
   * wants SPNEGO, so JMX answers as it does without Kerberos.
   *
   * <p>The Hive CLI's {@code CREATE TABLE} used to create the warehouse as the image's {@code
   * hive/<host>} principal. The client logs in as that principal from the container's keytab, so
   * the directory gets the same owner ({@code hadoop}, the image's mapping of two-part principals),
   * the group it inherits from {@code /} and mode 755, which keeps it closed to other users as
   * HiveUserAuthenticationIT expects.
   *
   * <p>The metastores are checked by port. The test classpath has no Hive metastore client, and a
   * metastore only listens once it has logged in from its keytab and opened its database.
   */
  private WaitStrategy kerberosReadiness() {
    return new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(liveDataNode(new AtomicInteger()))
        .withStrategy(
            new ProbeWaitStrategy(
                    "Kerberos HDFS to create " + WAREHOUSE_DIR, this::createKerberosWarehouse)
                .withPollInterval(Duration.ofSeconds(2)))
        .withStrategy(portProbe(HIVE_METASTORE_PORT, "Hive Metastore"))
        // MultipleHMSUserAuthenticationIT connects to the image's second metastore.
        .withStrategy(portProbe(SECOND_HIVE_METASTORE_PORT, "the second Hive Metastore"));
  }

  // The keytab and krb5.conf exist before the NameNode starts, so they are there once it is live.
  private boolean createKerberosWarehouse(WaitStrategyTarget target)
      throws IOException, InterruptedException {
    if (!hasHdfsClient()) {
      // Such a module only reaches HDFS through the metastore, which writes as the superuser.
      LOG.warn("No HDFS client on the test classpath; not creating {}", WAREHOUSE_DIR);
      return true;
    }
    Path dir = Files.createTempDirectory("kerberos-hive-");
    try {
      String keytab = dir.resolve("admin.keytab").toString();
      target.copyFileFromContainer("/etc/admin.keytab", keytab);
      String ip = ipAddress(target.getContainerInfo());
      String krb5Conf =
          target.copyFileFromContainer(
              "/etc/krb5.conf", in -> new String(in.readAllBytes(), StandardCharsets.UTF_8));
      Path krb5ConfPath =
          Files.writeString(dir.resolve("krb5.conf"), KerberosHdfs.reachableKrb5Conf(krb5Conf, ip));
      KerberosHdfs hdfs =
          new KerberosHdfs(format("hdfs://%s:%d", ip, HDFS_DEFAULTFS_PORT), hostName, keytab);
      KerberosHdfs.withKrb5Conf(
          krb5ConfPath.toString(),
          () -> {
            hdfs.mkdirs(WAREHOUSE_DIR);
            hdfs.setPermission(WAREHOUSE_DIR, "755", false);
          });
      return true;
    } finally {
      try (Stream<Path> files = Files.walk(dir)) {
        files.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
  }

  private static boolean hasHdfsClient() {
    try {
      Class.forName("org.apache.hadoop.hdfs.DistributedFileSystem");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  // Polls the NameNode's JMX on either web port and remembers the one that answered. Logs the
  // last response if no DataNode registers in time.
  private static WaitStrategy liveDataNode(AtomicInteger namenodePort) {
    AtomicReference<String> lastResponse = new AtomicReference<>();
    return new ProbeWaitStrategy(
            "HDFS to report a live DataNode",
            target -> {
              for (int port : NAMENODE_HTTP_PORTS) {
                String jmx =
                    httpRequest(
                        "GET",
                        format(
                            "http://%s:%d/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState",
                            ipAddress(target.getContainerInfo()), port));
                if (jmx != null) {
                  lastResponse.set(jmx);
                }
                if (hasLiveDataNode(jmx)) {
                  namenodePort.set(port);
                  return true;
                }
              }
              return false;
            })
        .withPollInterval(Duration.ofMillis(500))
        .withTimeoutReport(
            target -> LOG.info("Last NameNode FSNamesystemState: {}", lastResponse.get()));
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
    // Package-private so that tests can read what a spec asked for.
    List<String> kerberosPrincipals = List.of();
    private boolean hostReachableHdfs;

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
      Optional<TestEndpoints.Hive> external = ExternalEndpoints.forService("hive", "");
      if (external.isPresent()) {
        return new HiveContainer(external.get());
      }
      if (!hostReachableHdfs) {
        return new HiveContainer(
            generateImageName(),
            hostName,
            exposePorts,
            extraHosts,
            filesToMount,
            envVars,
            network,
            kerberosEnabled,
            kerberosPrincipals);
      }
      Preconditions.checkState(!kerberosEnabled, "Host-reachable HDFS is for the plain Hive");
      Set<Integer> ports =
          exposePorts.stream()
              .filter(port -> port != HDFS_DEFAULTFS_PORT)
              .collect(ImmutableSet.toImmutableSet());
      return new HiveContainer(
          generateImageName(),
          "localhost",
          ports,
          extraHosts,
          filesToMount,
          envVars,
          network,
          false,
          kerberosPrincipals,
          freePort(),
          freePort());
    }

    /**
     * Makes HDFS reachable from the test JVM through the Docker host: the NameNode and DataNode
     * listen on per-run ports that are published unchanged, and the container's host name is {@code
     * localhost}, so an HDFS URI is valid both in the test JVM and inside the container, where the
     * metastore creates directories at the URIs tests send. Not for a Hive other containers must
     * reach by name.
     *
     * @return this builder
     */
    public Builder withHostReachableHdfs() {
      this.hostReachableHdfs = true;
      return this;
    }

    /**
     * Sets the extra Kerberos principals a Kerberos Hive creates when it starts.
     *
     * @param kerberosPrincipals the principal names, without realm; see {@link
     *     HiveFixture#kerberosPrincipals()}
     * @return this builder
     */
    public Builder withKerberosPrincipals(Collection<String> kerberosPrincipals) {
      this.kerberosPrincipals = List.copyOf(kerberosPrincipals);
      return this;
    }
  }
}
