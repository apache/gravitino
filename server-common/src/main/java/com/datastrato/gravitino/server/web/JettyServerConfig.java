/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;

public final class JettyServerConfig {

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
          .createWithDefault(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100));

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

  private final String host;

  private final int httpPort;

  private final int minThreads;

  private final int maxThreads;

  private final long stopTimeout;

  private final int idleTimeout;

  private final int requestHeaderSize;

  private final int responseHeaderSize;

  private final int threadPoolWorkQueueSize;

  private final Config internalConfig;

  private JettyServerConfig(Map<String, String> configs) {
    this.internalConfig = new Config(false) {};
    internalConfig.loadFromMap(configs, t -> true);

    this.host = internalConfig.get(WEBSERVER_HOST);
    this.httpPort = internalConfig.get(WEBSERVER_HTTP_PORT);
    this.minThreads = internalConfig.get(WEBSERVER_MIN_THREADS);
    this.maxThreads = internalConfig.get(WEBSERVER_MAX_THREADS);
    this.stopTimeout = internalConfig.get(WEBSERVER_STOP_TIMEOUT);
    this.idleTimeout = internalConfig.get(WEBSERVER_IDLE_TIMEOUT);
    this.requestHeaderSize = internalConfig.get(WEBSERVER_REQUEST_HEADER_SIZE);
    this.responseHeaderSize = internalConfig.get(WEBSERVER_RESPONSE_HEADER_SIZE);
    this.threadPoolWorkQueueSize = internalConfig.get(WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE);
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
}
