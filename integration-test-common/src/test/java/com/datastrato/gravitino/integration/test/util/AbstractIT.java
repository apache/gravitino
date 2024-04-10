/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.dto.util.DTOConverters.toDTO;
import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.integration.test.MiniGravitino;
import com.datastrato.gravitino.integration.test.MiniGravitinoContext;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.indexes.Index;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;

@ExtendWith(PrintFuncNameExtension.class)
public class AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  protected static GravitinoAdminClient client;

  private static final OAuthMockDataProvider mockDataProvider = OAuthMockDataProvider.getInstance();

  protected static final CloseableGroup closer = CloseableGroup.create();

  private static MiniGravitino miniGravitino;

  protected static Config serverConfig;

  public static String testMode = "";

  protected static Map<String, String> customConfigs = new HashMap<>();

  protected static boolean ignoreIcebergRestService = true;

  private static final String MYSQL_DOCKER_IMAGE_VERSION = "mysql:8.0";
  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.26/mysql-connector-java-8.0.26.jar";

  private static final String META_DATA = "metadata";
  private static MySQLContainer<?> MYSQL_CONTAINER;

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
    String mysqlUrl = MYSQL_CONTAINER.getJdbcUrl();
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

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    testMode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    LOG.info("Running Gravitino Server in {} mode", testMode);

    if ("true".equals(System.getenv("jdbcBackend"))) {
      // Start MySQL docker instance.
      MYSQL_CONTAINER =
          new MySQLContainer<>(MYSQL_DOCKER_IMAGE_VERSION)
              .withDatabaseName(META_DATA)
              .withUsername("root")
              .withPassword("root");
      MYSQL_CONTAINER.start();

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
      client =
          GravitinoAdminClient.builder(serverUri)
              .withKerberosAuth(KerberosProviderHelper.getProvider())
              .build();
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
    customConfigs.clear();
    LOG.info("Tearing down Gravitino Server");

    if (MYSQL_CONTAINER != null) {
      MYSQL_CONTAINER.stop();
    }
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

  protected static void assertionsTableInfo(
      String tableName,
      String tableComment,
      List<Column> columns,
      Map<String, String> properties,
      Index[] indexes,
      Table table) {
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(tableComment, table.comment());
    Assertions.assertEquals(columns.size(), table.columns().length);
    for (int i = 0; i < columns.size(); i++) {
      assertColumn(columns.get(i), table.columns()[i]);
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertEquals(entry.getValue(), table.properties().get(entry.getKey()));
    }
    if (ArrayUtils.isNotEmpty(indexes)) {
      Assertions.assertEquals(indexes.length, table.index().length);

      Map<String, Index> indexByName =
          Arrays.stream(indexes).collect(Collectors.toMap(Index::name, index -> index));

      for (int i = 0; i < table.index().length; i++) {
        Assertions.assertTrue(indexByName.containsKey(table.index()[i].name()));
        Assertions.assertEquals(
            indexByName.get(table.index()[i].name()).type(), table.index()[i].type());
        for (int j = 0; j < table.index()[i].fieldNames().length; j++) {
          for (int k = 0; k < table.index()[i].fieldNames()[j].length; k++) {
            Assertions.assertEquals(
                indexByName.get(table.index()[i].name()).fieldNames()[j][k],
                table.index()[i].fieldNames()[j][k]);
          }
        }
      }
    }
  }

  protected static void assertColumn(Column expected, Column actual) {
    if (!(actual instanceof ColumnDTO)) {
      actual = toDTO(actual);
    }
    if (!(expected instanceof ColumnDTO)) {
      expected = toDTO(expected);
    }

    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.dataType(), actual.dataType());
    Assertions.assertEquals(expected.nullable(), actual.nullable());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.autoIncrement(), actual.autoIncrement());
    if (expected.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET) && expected.nullable()) {
      Assertions.assertEquals(LiteralDTO.NULL, actual.defaultValue());
    } else {
      Assertions.assertEquals(expected.defaultValue(), actual.defaultValue());
    }
  }
}
