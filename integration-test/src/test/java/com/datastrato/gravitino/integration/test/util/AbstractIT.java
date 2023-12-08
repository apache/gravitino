/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.integration.test.MiniGravitino;
import com.datastrato.gravitino.integration.test.MiniGravitinoContext;
import com.datastrato.gravitino.server.GravitinoServer;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.auth.OAuthConfig;
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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
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
        .equals(customConfigs.get(OAuthConfig.AUTHENTICATOR.getKey()))) {
      client = GravitinoClient.builder(uri).withOAuth(mockDataProvider).build();
    } else {
      client = GravitinoClient.builder(uri).build();
    }
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    if (client != null) {
      client.close();
    }
    if (testMode != null && testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      miniGravitino.stop();
    } else {
      GravitinoITUtils.stopGravitinoServer();
      recoverGravitinoServerConfig();
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
