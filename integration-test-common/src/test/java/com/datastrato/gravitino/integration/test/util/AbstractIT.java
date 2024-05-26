/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.integration.test.MiniGravitino;
import com.datastrato.gravitino.integration.test.MiniGravitinoContext;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.server.GravitinoServer;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith({PrintFuncNameExtension.class, CloseContainerExtension.class})
public class AbstractIT {
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  protected static GravitinoAdminClient client;

  private static final OAuthMockDataProvider mockDataProvider = OAuthMockDataProvider.getInstance();

  protected static final CloseableGroup closer = CloseableGroup.create();

  private static MiniGravitino miniGravitino;

  protected static Config serverConfig;

  public static String testMode = "";

  protected static Map<String, String> customConfigs = new HashMap<>();

  protected static boolean ignoreIcebergRestService = true;

  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.26/mysql-connector-java-8.0.26.jar";

  private static TestDatabaseName META_DATA;
  private static MySQLContainer MYSQL_CONTAINER;

  protected static String serverUri;

  public static int getGravitinoServerPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
    return jettyServerConfig.getHttpPort();
  }

  public static void registerCustomConfigs(Map<String, String> configs) {
    customConfigs.putAll(configs);
  }

  private static void rewriteGravitinoServerConfig() throws IOException {
    if (customConfigs.isEmpty()) return;

    String gravitinoHome = System.getenv("GRAVITINO_HOME");

    String tmpFileName = GravitinoServer.CONF_FILE + ".tmp";
    Path tmpPath = Paths.get(gravitinoHome, "conf", tmpFileName);
    Files.deleteIfExists(tmpPath);

    Path configPath = Paths.get(gravitinoHome, "conf", GravitinoServer.CONF_FILE);
    Files.move(configPath, tmpPath);

    ITUtils.rewriteConfigFile(tmpPath.toString(), configPath.toString(), customConfigs);
  }

  private static void recoverGravitinoServerConfig() throws IOException {
    if (customConfigs.isEmpty()) return;

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String tmpFileName = GravitinoServer.CONF_FILE + ".tmp";
    Path tmpPath = Paths.get(gravitinoHome, "conf", tmpFileName);
    Path configPath = Paths.get(gravitinoHome, "conf", GravitinoServer.CONF_FILE);
    Files.deleteIfExists(configPath);
    Files.move(tmpPath, configPath);
  }

  protected static void downLoadMySQLDriver(String relativeDeployLibsPath) throws IOException {
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      java.nio.file.Path tmpPath = Paths.get(gravitinoHome, relativeDeployLibsPath);
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }
  }

  private static void setMySQLBackend() {
    String mysqlUrl = MYSQL_CONTAINER.getJdbcUrl(META_DATA);
    customConfigs.put(Configs.ENTITY_STORE_KEY, "relational");
    customConfigs.put(Configs.ENTITY_RELATIONAL_STORE_KEY, "JDBCBackend");
    customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY, mysqlUrl);
    customConfigs.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY, "root");
    customConfigs.put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY, "root");

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
                          "/scripts/mysql/schema-%s-mysql.sql", ConfigConstants.VERSION_0_5_0)),
              "UTF-8");
      String[] initMySQLBackendSqls = mysqlContent.split(";");
      initMySQLBackendSqls = ArrayUtils.addFirst(initMySQLBackendSqls, "use " + META_DATA + ";");
      for (String sql : initMySQLBackendSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      LOG.error("Failed to create database in mysql", e);
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "embedded, jdbcBackend",
    "embedded, kvBackend",
    "deploy, jdbcBackend",
    "deploy, kvBackend"
  })
  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    testMode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    LOG.info("Running Gravitino Server in {} mode", testMode);

    if ("true".equals(System.getenv("jdbcBackend"))) {
      // Start MySQL docker instance.
      META_DATA = TestDatabaseName.MYSQL_JDBC_BACKEND;
      containerSuite.startMySQLContainer(META_DATA);
      MYSQL_CONTAINER = containerSuite.getMySQLContainer();

      setMySQLBackend();
    }

    serverConfig = new ServerConfig();
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      MiniGravitinoContext context =
          new MiniGravitinoContext(customConfigs, ignoreIcebergRestService);
      miniGravitino = new MiniGravitino(context);
      miniGravitino.start();
      serverConfig = miniGravitino.getServerConfig();
    } else {
      rewriteGravitinoServerConfig();
      serverConfig.loadFromFile(GravitinoServer.CONF_FILE);
      downLoadMySQLDriver("/libs");
      try {
        FileUtils.deleteDirectory(
            FileUtils.getFile(serverConfig.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)));
      } catch (Exception e) {
        // Ignore
      }

      GravitinoITUtils.startGravitinoServer();
    }

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    serverUri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();
    if (AuthenticatorType.OAUTH
        .name()
        .toLowerCase()
        .equals(customConfigs.get(Configs.AUTHENTICATOR.getKey()))) {
      client = GravitinoAdminClient.builder(serverUri).withOAuth(mockDataProvider).build();
    } else if (AuthenticatorType.SIMPLE
        .name()
        .toLowerCase()
        .equals(customConfigs.get(Configs.AUTHENTICATOR.getKey()))) {
      client = GravitinoAdminClient.builder(serverUri).withSimpleAuth().build();
    } else if (AuthenticatorType.KERBEROS
        .name()
        .toLowerCase()
        .equals(customConfigs.get(Configs.AUTHENTICATOR.getKey()))) {
      serverUri = "http://localhost:" + jettyServerConfig.getHttpPort();
      client = null;
    } else {
      client = GravitinoAdminClient.builder(serverUri).build();
    }
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE) && miniGravitino != null) {
      miniGravitino.stop();
    } else {
      GravitinoITUtils.stopGravitinoServer();
      recoverGravitinoServerConfig();
    }
    if (client != null) {
      client.close();
    }

    LOG.info("Tearing down Gravitino Server");
  }

  public static GravitinoAdminClient getGravitinoClient() {
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
}
