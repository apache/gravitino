/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JettyServerConfig {
  private static final Logger LOG = LoggerFactory.getLogger(JettyServerConfig.class);
  private static final String SPLITTER = ",";

  public static final ConfigEntry<String> WEBSERVER_HOST =
      new ConfigBuilder("host")
          .doc("The host name of the Jetty web server")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("0.0.0.0");

  public static final ConfigEntry<Integer> WEBSERVER_HTTP_PORT =
      new ConfigBuilder("httpPort")
          .doc("The http port number of the Jetty web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(8090);

  public static final ConfigEntry<Integer> WEBSERVER_MIN_THREADS =
      new ConfigBuilder("minThreads")
          .doc("The minimum number of threads in the thread pool used by Jetty webserver")
          .version("0.2.0")
          .intConf()
          .createWithDefault(
              Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 4));

  public static final ConfigEntry<Integer> WEBSERVER_MAX_THREADS =
      new ConfigBuilder("maxThreads")
          .doc("The maximum number of threads in the thread pool used by Jetty webserver")
          .version("0.1.0")
          .intConf()
          .createWithDefault(Math.max(Runtime.getRuntime().availableProcessors() * 4, 400));

  public static final ConfigEntry<Long> WEBSERVER_STOP_TIMEOUT =
      new ConfigBuilder("stopTimeout")
          .doc("Time in milliseconds to gracefully shutdown the Jetty webserver")
          .version("0.2.0")
          .longConf()
          .createWithDefault(30 * 1000L);

  public static final ConfigEntry<Integer> WEBSERVER_IDLE_TIMEOUT =
      new ConfigBuilder("idleTimeout")
          .doc("The timeout in milliseconds of idle connections")
          .version("0.2.0")
          .intConf()
          .createWithDefault(30 * 1000);

  public static final ConfigEntry<Integer> WEBSERVER_REQUEST_HEADER_SIZE =
      new ConfigBuilder("requestHeaderSize")
          .doc("Maximum size of HTTP requests")
          .version("0.1.0")
          .intConf()
          .createWithDefault(128 * 1024);

  public static final ConfigEntry<Integer> WEBSERVER_RESPONSE_HEADER_SIZE =
      new ConfigBuilder("responseHeaderSize")
          .doc("Maximum size of HTTP responses")
          .version("0.1.0")
          .intConf()
          .createWithDefault(128 * 1024);

  public static final ConfigEntry<Integer> WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE =
      new ConfigBuilder("threadPoolWorkQueueSize")
          .doc("The size of the queue in the thread pool used by Jetty webserver")
          .version("0.1.0")
          .intConf()
          .createWithDefault(100);

  public static final ConfigEntry<Boolean> ENABLE_HTTPS =
      new ConfigBuilder("enableHttps")
          .doc("Enables https")
          .version("0.3.0")
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<Integer> WEBSERVER_HTTPS_PORT =
      new ConfigBuilder("httpsPort")
          .doc("The https port number of the Jetty web server")
          .version("0.3.0")
          .intConf()
          .createWithDefault(8433);

  public static final ConfigEntry<String> SSL_KEYSTORE_PATH =
      new ConfigBuilder("keyStorePath")
          .doc("Path to the key store file")
          .version("0.3.0")
          .stringConf()
          .create();

  public static final ConfigEntry<String> SSL_KEYSTORE_PASSWORD =
      new ConfigBuilder("keyStorePassword")
          .doc("Password to the key store")
          .version("0.3.0")
          .stringConf()
          .create();

  public static final ConfigEntry<String> SSL_MANAGER_PASSWORD =
      new ConfigBuilder("managerPassword")
          .doc("Manager password to the key store")
          .version("0.3.0")
          .stringConf()
          .create();

  public static final ConfigEntry<String> SSL_KEYSTORE_TYPE =
      new ConfigBuilder("keyStoreType")
          .doc("The type to the key store")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("JKS");

  public static final ConfigEntry<Optional<String>> SSL_PROTOCOL =
      new ConfigBuilder("tlsProtocol")
          .doc("TLS protocol to use. The protocol must be supported by JVM")
          .version("0.3.0")
          .stringConf()
          .createWithOptional();
  public static final ConfigEntry<String> ENABLE_CIPHER_ALGORITHMS =
      new ConfigBuilder("enableCipherAlgorithms")
          .doc("The collection of the cipher algorithms are enabled ")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("");

  public static final ConfigEntry<Boolean> ENABLE_CLIENT_AUTH =
      new ConfigBuilder("enableClientAuth")
          .doc("Enables the authentication of the client")
          .version("0.3.0")
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<String> SSL_TRUST_STORE_PATH =
      new ConfigBuilder("trustStorePath")
          .doc("Path to the trust store file")
          .version("0.3.0")
          .stringConf()
          .create();

  public static final ConfigEntry<String> SSL_TRUST_STORE_PASSWORD =
      new ConfigBuilder("trustStorePassword")
          .doc("Password to the trust store")
          .version("0.3.0")
          .stringConf()
          .create();

  public static final ConfigEntry<String> SSL_TRUST_STORE_TYPE =
      new ConfigBuilder("trustStoreType")
          .doc("The type to the trust store")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("JKS");

  private final String host;

  private final int httpPort;

  private final int minThreads;

  private final int maxThreads;

  private final long stopTimeout;

  private final int idleTimeout;

  private final int requestHeaderSize;

  private final int responseHeaderSize;

  private final int threadPoolWorkQueueSize;

  private final int httpsPort;
  private final String keyStorePath;
  private final String keyStorePassword;
  private final String managerPassword;
  private final boolean enableHttps;
  private final String keyStoreType;
  private final Optional<String> tlsProtocol;
  private final Set<String> enableCipherAlgorithms;
  private final boolean enableClientAuth;
  private final String trustStorePath;
  private final String trustStorePasword;
  private final String trustStoreType;
  private final Config internalConfig;

  private JettyServerConfig(Map<String, String> configs) {
    this.internalConfig = new Config(false) {};
    internalConfig.loadFromMap(configs, t -> true);

    this.host = internalConfig.get(WEBSERVER_HOST);
    this.httpPort = internalConfig.get(WEBSERVER_HTTP_PORT);

    int minThreads = internalConfig.get(WEBSERVER_MIN_THREADS);
    int maxThreads = internalConfig.get(WEBSERVER_MAX_THREADS);
    Preconditions.checkArgument(
        maxThreads >= minThreads,
        String.format("maxThreads:%d should not less than minThreads:%d", maxThreads, minThreads));
    // at lease acceptor thread + select thread + 1 (worker thread)
    if (minThreads < 8) {
      LOG.info("The configuration of minThread is too small, adjust to 8");
      minThreads = 8;
    }
    if (maxThreads < 8) {
      LOG.info("The configuration of maxThread is too small, adjust to 8");
      maxThreads = 8;
    }
    this.minThreads = minThreads;
    this.maxThreads = maxThreads;

    this.stopTimeout = internalConfig.get(WEBSERVER_STOP_TIMEOUT);
    this.idleTimeout = internalConfig.get(WEBSERVER_IDLE_TIMEOUT);
    this.requestHeaderSize = internalConfig.get(WEBSERVER_REQUEST_HEADER_SIZE);
    this.responseHeaderSize = internalConfig.get(WEBSERVER_RESPONSE_HEADER_SIZE);
    this.threadPoolWorkQueueSize = internalConfig.get(WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE);

    this.enableHttps = internalConfig.get(ENABLE_HTTPS);
    this.httpsPort = internalConfig.get(WEBSERVER_HTTPS_PORT);
    this.keyStorePath = internalConfig.get(SSL_KEYSTORE_PATH);
    this.keyStorePassword = internalConfig.get(SSL_KEYSTORE_PASSWORD);
    this.managerPassword = internalConfig.get(SSL_MANAGER_PASSWORD);
    this.keyStoreType = internalConfig.get(SSL_KEYSTORE_TYPE);
    this.tlsProtocol = internalConfig.get(SSL_PROTOCOL);
    this.enableCipherAlgorithms =
        Collections.unmodifiableSet(
            Sets.newHashSet(internalConfig.get(ENABLE_CIPHER_ALGORITHMS).split(SPLITTER)));
    this.enableClientAuth = internalConfig.get(ENABLE_CLIENT_AUTH);
    this.trustStorePath = internalConfig.get(SSL_TRUST_STORE_PATH);
    this.trustStorePasword = internalConfig.get(SSL_TRUST_STORE_PASSWORD);
    this.trustStoreType = internalConfig.get(SSL_TRUST_STORE_TYPE);
  }

  public static JettyServerConfig fromConfig(Config config, String prefix) {
    Map<String, String> configs = config.getConfigsWithPrefix(prefix);
    return new JettyServerConfig(configs);
  }

  public static JettyServerConfig fromConfig(Config config) {
    return fromConfig(config, "");
  }

  public String getHost() {
    return host;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public int getMinThreads() {
    return minThreads;
  }

  public int getMaxThreads() {
    return maxThreads;
  }

  public long getStopTimeout() {
    return stopTimeout;
  }

  public int getRequestHeaderSize() {
    return requestHeaderSize;
  }

  public int getResponseHeaderSize() {
    return responseHeaderSize;
  }

  public int getThreadPoolWorkQueueSize() {
    return threadPoolWorkQueueSize;
  }

  public int getIdleTimeout() {
    return idleTimeout;
  }

  public int getHttpsPort() {
    return httpsPort;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getManagerPassword() {
    return managerPassword;
  }

  public boolean isEnableHttps() {
    return enableHttps;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  public Optional<String> getTlsProtocol() {
    return tlsProtocol;
  }

  public boolean isEnableClientAuth() {
    return enableClientAuth;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePasword() {
    return trustStorePasword;
  }

  public String getTrustStoreType() {
    return trustStoreType;
  }

  private SSLContext getDefaultSSLContext() {
    try {
      return SSLContext.getDefault();
    } catch (NoSuchAlgorithmException nsa) {
      return null;
    }
  }

  private SSLContext getSSLContextInstance(String protocol) {
    try {
      SSLContext context = SSLContext.getInstance(protocol);
      context.init(null, null, null);
      return context;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      return null;
    }
  }

  public Set<String> getSupportedAlgorithms() {
    if (enableCipherAlgorithms.isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> supportedAlgorithms = Sets.newHashSet(enableCipherAlgorithms);
    supportedAlgorithms.retainAll(getSupportedCipherSuites());
    return supportedAlgorithms;
  }

  @VisibleForTesting
  Set<String> getSupportedCipherSuites() {
    SSLContext context =
        tlsProtocol.map(this::getSSLContextInstance).orElseGet(this::getDefaultSSLContext);
    if (context == null) {
      return Collections.emptySet();
    }
    return Sets.newHashSet(context.getServerSocketFactory().getSupportedCipherSuites());
  }
}
