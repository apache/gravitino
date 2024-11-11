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
package org.apache.gravitino.integration.test.util;

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.integration.test.util.TestDatabaseName.PG_CATALOG_POSTGRESQL_IT;
import static org.apache.gravitino.integration.test.util.TestDatabaseName.PG_JDBC_BACKEND;
import static org.apache.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.integration.test.MiniGravitino;
import org.apache.gravitino.integration.test.MiniGravitinoContext;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

/**
 * BaseIT can be used as a base class for integration tests. It will automatically start a Gravitino
 * server and stop it after all tests are finished.
 *
 * <p>Another use case is to start a MySQL or PostgreSQL docker instance and create a database for
 * testing or just start the Gravitino server manually.
 */
@ExtendWith({PrintFuncNameExtension.class, CloseContainerExtension.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseIT {

  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final Logger LOG = LoggerFactory.getLogger(BaseIT.class);
  private static final Splitter COMMA = Splitter.on(",").omitEmptyStrings().trimResults();

  protected GravitinoAdminClient client;

  private static final OAuthMockDataProvider mockDataProvider = OAuthMockDataProvider.getInstance();

  protected static final CloseableGroup closer = CloseableGroup.create();

  private MiniGravitino miniGravitino;

  protected Config serverConfig;

  public String testMode = "";

  protected Map<String, String> customConfigs = new HashMap<>();

  protected boolean ignoreIcebergRestService = true;

  public static final String DOWNLOAD_MYSQL_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.26/mysql-connector-java-8.0.26.jar";

  public static final String DOWNLOAD_POSTGRESQL_JDBC_DRIVER_URL =
      "https://jdbc.postgresql.org/download/postgresql-42.7.0.jar";

  private TestDatabaseName META_DATA;
  private MySQLContainer MYSQL_CONTAINER;
  private PostgreSQLContainer POSTGRESQL_CONTAINER;

  protected String serverUri;

  protected String originConfig;

  public int getGravitinoServerPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
    return jettyServerConfig.getHttpPort();
  }

  public void registerCustomConfigs(Map<String, String> configs) {
    customConfigs.putAll(configs);
  }

  private void rewriteGravitinoServerConfig() throws IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Path configPath = Paths.get(gravitinoHome, "conf", GravitinoServer.CONF_FILE);
    if (originConfig == null) {
      originConfig = FileUtils.readFileToString(configPath.toFile(), StandardCharsets.UTF_8);
    }

    if (customConfigs.isEmpty()) {
      return;
    }

    String tmpFileName = GravitinoServer.CONF_FILE + ".tmp";
    Path tmpPath = Paths.get(gravitinoHome, "conf", tmpFileName);
    Files.deleteIfExists(tmpPath);

    Files.move(configPath, tmpPath);
    ITUtils.rewriteConfigFile(tmpPath.toString(), configPath.toString(), customConfigs);
  }

  private void recoverGravitinoServerConfig() throws IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Path configPath = Paths.get(gravitinoHome, "conf", GravitinoServer.CONF_FILE);

    if (originConfig != null) {
      Files.deleteIfExists(configPath);
      FileUtils.write(configPath.toFile(), originConfig, StandardCharsets.UTF_8);
    }
  }

  protected void downLoadJDBCDriver() throws IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String serverPath = ITUtils.joinPath(gravitinoHome, "libs");
      String icebergCatalogPath =
          ITUtils.joinPath(gravitinoHome, "catalogs", "lakehouse-iceberg", "libs");
      DownloaderUtils.downloadFile(DOWNLOAD_MYSQL_JDBC_DRIVER_URL, serverPath, icebergCatalogPath);
      DownloaderUtils.downloadFile(
          DOWNLOAD_POSTGRESQL_JDBC_DRIVER_URL, serverPath, icebergCatalogPath);
    } else {
      Path icebergLibsPath =
          Paths.get(gravitinoHome, "catalogs", "catalog-lakehouse-iceberg", "build", "libs");
      DownloaderUtils.downloadFile(DOWNLOAD_MYSQL_JDBC_DRIVER_URL, icebergLibsPath.toString());

      DownloaderUtils.downloadFile(DOWNLOAD_POSTGRESQL_JDBC_DRIVER_URL, icebergLibsPath.toString());
    }
  }

  public String startAndInitPGBackend() {
    META_DATA = PG_JDBC_BACKEND;
    containerSuite.startPostgreSQLContainer(META_DATA);
    POSTGRESQL_CONTAINER = containerSuite.getPostgreSQLContainer();

    String pgUrlWithoutSchema = POSTGRESQL_CONTAINER.getJdbcUrl(META_DATA);
    String randomSchemaName = RandomStringUtils.random(10, true, false);
    // Connect to the PostgreSQL docker and create a schema
    String currentExecuteSql = "";
    try (Connection connection =
        DriverManager.getConnection(
            pgUrlWithoutSchema,
            POSTGRESQL_CONTAINER.getUsername(),
            POSTGRESQL_CONTAINER.getPassword())) {
      connection.setCatalog(PG_CATALOG_POSTGRESQL_IT.toString());
      final Statement statement = connection.createStatement();
      statement.execute("drop schema if exists " + randomSchemaName);
      statement.execute("create schema " + randomSchemaName);
      statement.execute("set search_path to " + randomSchemaName);
      String gravitinoHome = System.getenv("GRAVITINO_ROOT_DIR");
      String mysqlContent =
          FileUtils.readFileToString(
              new File(
                  gravitinoHome
                      + String.format(
                          "/scripts/postgresql/schema-%s-postgresql.sql",
                          ConfigConstants.VERSION_0_7_0)),
              "UTF-8");

      String[] initPGBackendSqls =
          Arrays.stream(mysqlContent.split(";"))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .toArray(String[]::new);

      for (String sql : initPGBackendSqls) {
        currentExecuteSql = sql;
        statement.execute(sql);
      }
    } catch (Exception e) {
      LOG.error("Failed to create database in pg, sql:\n{}", currentExecuteSql, e);
      throw new RuntimeException(e);
    }

    pgUrlWithoutSchema = pgUrlWithoutSchema + "?currentSchema=" + randomSchemaName;
    customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY, pgUrlWithoutSchema);

    LOG.info("PG URL: {}", pgUrlWithoutSchema);
    return pgUrlWithoutSchema;
  }

  public String startAndInitMySQLBackend() {
    META_DATA = TestDatabaseName.MYSQL_JDBC_BACKEND;
    containerSuite.startMySQLContainer(META_DATA);
    MYSQL_CONTAINER = containerSuite.getMySQLContainer();

    String mysqlUrl = MYSQL_CONTAINER.getJdbcUrl(META_DATA);
    LOG.info("MySQL URL: {}", mysqlUrl);
    // Connect to the mysql docker and create a databases
    try (Connection connection =
            DriverManager.getConnection(
                StringUtils.substring(mysqlUrl, 0, mysqlUrl.lastIndexOf("/")), "root", "root");
        final Statement statement = connection.createStatement()) {
      statement.execute("drop database if exists " + META_DATA);
      statement.execute("create database " + META_DATA);
      String gravitinoHome = System.getenv("GRAVITINO_ROOT_DIR");
      String mysqlContent =
          FileUtils.readFileToString(
              new File(
                  gravitinoHome
                      + String.format(
                          "/scripts/mysql/schema-%s-mysql.sql", ConfigConstants.VERSION_0_7_0)),
              "UTF-8");

      String[] initMySQLBackendSqls =
          Arrays.stream(mysqlContent.split(";"))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .toArray(String[]::new);

      initMySQLBackendSqls = ArrayUtils.addFirst(initMySQLBackendSqls, "use " + META_DATA + ";");
      for (String sql : initMySQLBackendSqls) {
        statement.execute(sql);
      }
      return mysqlUrl;
    } catch (Exception e) {
      LOG.error("Failed to create database in mysql", e);
      throw new RuntimeException(e);
    }
  }

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    testMode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    LOG.info("Running Gravitino Server in {} mode", testMode);

    if ("MySQL".equalsIgnoreCase(System.getenv("jdbcBackend"))) {
      // Start MySQL docker instance.
      String jdbcURL = startAndInitMySQLBackend();
      customConfigs.put(Configs.ENTITY_STORE_KEY, "relational");
      customConfigs.put(Configs.ENTITY_RELATIONAL_STORE_KEY, "JDBCBackend");
      customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY, jdbcURL);
      customConfigs.put(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
      customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY, "root");
      customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY, "root");
    } else if ("PostgreSQL".equalsIgnoreCase(System.getenv("jdbcBackend"))) {
      // Start PostgreSQL docker instance.
      String pgJdbcUrl = startAndInitPGBackend();
      customConfigs.put(Configs.ENTITY_STORE_KEY, "relational");
      customConfigs.put(Configs.ENTITY_RELATIONAL_STORE_KEY, "JDBCBackend");
      customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY, pgJdbcUrl);
      customConfigs.put(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY,
          POSTGRESQL_CONTAINER.getDriverClassName(META_DATA));
      customConfigs.put(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY, POSTGRESQL_CONTAINER.getUsername());
      customConfigs.put(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY, POSTGRESQL_CONTAINER.getPassword());
    }

    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.mkdir();
    file.deleteOnExit();

    serverConfig = new ServerConfig();
    customConfigs.put(ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(), file.getAbsolutePath());
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      MiniGravitinoContext context =
          new MiniGravitinoContext(customConfigs, ignoreIcebergRestService);
      miniGravitino = new MiniGravitino(context);
      miniGravitino.start();
      serverConfig = miniGravitino.getServerConfig();
    } else {
      rewriteGravitinoServerConfig();
      serverConfig.loadFromFile(GravitinoServer.CONF_FILE);
      downLoadJDBCDriver();

      GravitinoITUtils.startGravitinoServer();

      JettyServerConfig jettyServerConfig =
          JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
      String checkServerUrl =
          "http://"
              + jettyServerConfig.getHost()
              + ":"
              + jettyServerConfig.getHttpPort()
              + "/metrics";
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(() -> HttpUtils.isHttpServerUp(checkServerUrl));
    }

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    serverUri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();

    List<String> authenticators = new ArrayList<>();
    String authenticatorStr = customConfigs.get(Configs.AUTHENTICATORS.getKey());
    if (authenticatorStr != null) {
      authenticators = COMMA.splitToList(authenticatorStr);
    }

    if (authenticators.contains(AuthenticatorType.OAUTH.name().toLowerCase())) {
      client = GravitinoAdminClient.builder(serverUri).withOAuth(mockDataProvider).build();
    } else if (authenticators.contains(AuthenticatorType.SIMPLE.name().toLowerCase())) {
      String userName = customConfigs.get("SimpleAuthUserName");
      if (userName != null) {
        client = GravitinoAdminClient.builder(serverUri).withSimpleAuth(userName).build();
      } else {
        client = GravitinoAdminClient.builder(serverUri).withSimpleAuth().build();
      }
    } else if (authenticators.contains(AuthenticatorType.KERBEROS.name().toLowerCase())) {
      serverUri = "http://localhost:" + jettyServerConfig.getHttpPort();
      client = null;
    } else {
      client = GravitinoAdminClient.builder(serverUri).build();
    }
  }

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE) && miniGravitino != null) {
      miniGravitino.stop();
    } else {
      GravitinoITUtils.stopGravitinoServer();
      recoverGravitinoServerConfig();
    }
    if (client != null) {
      client.close();
    }
    customConfigs.clear();
    LOG.info("Tearing down Gravitino Server");
  }

  public GravitinoAdminClient getGravitinoClient() {
    return client;
  }

  protected String readGitCommitIdFromGitFile() {
    try {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      String gitFolder = gravitinoHome + File.separator + ".git" + File.separator;
      String headFileContent = FileUtils.readFileToString(new File(gitFolder + "HEAD"), "UTF-8");
      String[] refAndBranch = headFileContent.split(":");
      if (refAndBranch.length == 1) {
        return refAndBranch[0].trim();
      }
      return FileUtils.readFileToString(new File(gitFolder + refAndBranch[1].trim()), "UTF-8")
          .trim();
    } catch (IOException e) {
      LOG.warn("Can't get git commit id for:", e);
      return "";
    }
  }

  private static boolean isDeploy() {
    String mode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    return Objects.equals(mode, ITUtils.DEPLOY_TEST_MODE);
  }

  public static void copyBundleJarsToDirectory(String bundleName, String directory) {
    String bundleJarSourceFile = ITUtils.getBundleJarSourceFile(bundleName);
    try {
      DownloaderUtils.downloadFile(bundleJarSourceFile, directory);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to copy the %s dependency jars: %s to %s",
              bundleName, bundleJarSourceFile, directory),
          e);
    }
  }

  protected static void copyBundleJarsToHadoop(String bundleName) {
    if (!isDeploy()) {
      return;
    }

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String hadoopLibDirs = ITUtils.joinPath(gravitinoHome, "catalogs", "hadoop", "libs");
    copyBundleJarsToDirectory(bundleName, hadoopLibDirs);
  }
}
