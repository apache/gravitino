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
import com.datastrato.gravitino.client.GravitinoClient;
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
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(PrintFuncNameExtension.class)
public class AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractIT.class);
  protected static GravitinoClient client;

  private static final OAuthMockDataProvider mockDataProvider = OAuthMockDataProvider.getInstance();

  protected static final CloseableGroup closer = CloseableGroup.create();

  private static MiniGravitino miniGravitino;

  protected static Config serverConfig;

  public static String testMode = "";

  protected static Map<String, String> customConfigs = new HashMap<>();

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

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    testMode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    LOG.info("Running Gravitino Server in {} mode", testMode);

    serverConfig = new ServerConfig();
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      MiniGravitinoContext context = new MiniGravitinoContext(customConfigs);
      miniGravitino = new MiniGravitino(context);
      miniGravitino.start();
      serverConfig = miniGravitino.getServerConfig();
    } else {
      rewriteGravitinoServerConfig();
      serverConfig.loadFromFile(GravitinoServer.CONF_FILE);
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

    String uri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();
    if (AuthenticatorType.OAUTH
        .name()
        .toLowerCase()
        .equals(customConfigs.get(Configs.AUTHENTICATOR.getKey()))) {
      client = GravitinoClient.builder(uri).withOAuth(mockDataProvider).build();
    } else if (AuthenticatorType.SIMPLE
        .name()
        .toLowerCase()
        .equals(customConfigs.get(Configs.AUTHENTICATOR.getKey()))) {
      client = GravitinoClient.builder(uri).withSimpleAuth().build();
    } else if (AuthenticatorType.KERBEROS
        .name()
        .toLowerCase()
        .equals(customConfigs.get(Configs.AUTHENTICATOR.getKey()))) {
      uri = "http://localhost:" + jettyServerConfig.getHttpPort();
      client =
          GravitinoClient.builder(uri)
              .withKerberosAuth(KerberosProviderHelper.getProvider())
              .build();
    } else {
      client = GravitinoClient.builder(uri).build();
    }
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
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

  // Get host IP from primary NIC
  protected static String getPrimaryNICIp() {
    String hostIP = "127.0.0.1";
    try {
      NetworkInterface networkInterface = NetworkInterface.getByName("en0"); // macOS
      if (networkInterface == null) {
        networkInterface = NetworkInterface.getByName("eth0"); // Linux and Windows
      }
      if (networkInterface != null) {
        Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          InetAddress address = addresses.nextElement();
          if (!address.isLoopbackAddress() && address.getHostAddress().indexOf(':') == -1) {
            hostIP = address.getHostAddress().replace("/", ""); // remove the first char '/'
            break;
          }
        }
      } else {
        InetAddress ip = InetAddress.getLocalHost();
        hostIP = ip.getHostAddress();
      }
    } catch (SocketException | UnknownHostException e) {
      LOG.error(e.getMessage(), e);
    }
    return hostIP;
  }

  public static GravitinoClient getGravitinoClient() {
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
      Assertions.assertEquals(columns.get(i).name(), table.columns()[i].name());
      Assertions.assertEquals(columns.get(i).dataType(), table.columns()[i].dataType());
      Assertions.assertEquals(columns.get(i).nullable(), table.columns()[i].nullable());
      Assertions.assertEquals(columns.get(i).comment(), table.columns()[i].comment());
      Assertions.assertEquals(columns.get(i).autoIncrement(), table.columns()[i].autoIncrement());
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
}
