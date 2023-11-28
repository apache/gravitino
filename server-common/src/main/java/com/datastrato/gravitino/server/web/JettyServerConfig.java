/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import com.google.common.base.Preconditions;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JettyServerConfig {
  private static final Logger LOG = LoggerFactory.getLogger(JettyServerConfig.class);

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
          .doc("The min thread size of the Jetty web server")
          .version("0.2.0")
          .intConf()
          .createWithDefault(
              Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 4));

  public static final ConfigEntry<Integer> WEBSERVER_MAX_THREADS =
      new ConfigBuilder("maxThreads")
          .doc("The max thread size of the Jetty web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(Math.max(Runtime.getRuntime().availableProcessors() * 4, 400));

  public static final ConfigEntry<Long> WEBSERVER_STOP_TIMEOUT =
      new ConfigBuilder("stopTimeout")
          .doc("The stop wait timeout of the Jetty web server")
          .version("0.2.0")
          .longConf()
          .createWithDefault(30 * 1000L);

  public static final ConfigEntry<Integer> WEBSERVER_IDLE_TIMEOUT =
      new ConfigBuilder("idleTimeout")
          .doc("The timeout of idle connections")
          .version("0.2.0")
          .intConf()
          .createWithDefault(30 * 1000);

  public static final ConfigEntry<Integer> WEBSERVER_REQUEST_HEADER_SIZE =
      new ConfigBuilder("requestHeaderSize")
          .doc("The request header size of the Jetty web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(128 * 1024);

  public static final ConfigEntry<Integer> WEBSERVER_RESPONSE_HEADER_SIZE =
      new ConfigBuilder("responseHeaderSize")
          .doc("The response header size of the Jetty web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(128 * 1024);

  public static final ConfigEntry<Integer> WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE =
      new ConfigBuilder("threadPoolWorkQueueSize")
          .doc("The executor thread pool work queue size of the Jetty web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(100);

  public static final ConfigEntry<Boolean> HTTPS_ENABLED =
      new ConfigBuilder("httpsEnabled")
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
          .createWithDefault("");

  public static final ConfigEntry<String> SSL_KEYSTORE_PASSWORD =
      new ConfigBuilder("keyStorePassword")
          .doc("Password to the key store")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("");

  public static final ConfigEntry<String> SSL_MANAGER_PASSWORD =
      new ConfigBuilder("managerPassword")
          .doc("Manager password to the key store")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("");

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
  private final boolean httpsEnabled;
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

    this.httpsEnabled = internalConfig.get(HTTPS_ENABLED);
    this.httpsPort = internalConfig.get(WEBSERVER_HTTPS_PORT);
    this.keyStorePath = internalConfig.get(SSL_KEYSTORE_PATH);
    this.keyStorePassword = internalConfig.get(SSL_KEYSTORE_PASSWORD);
    this.managerPassword = internalConfig.get(SSL_MANAGER_PASSWORD);
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

  public boolean isHttpsEnabled() {
    return httpsEnabled;
  }
}
