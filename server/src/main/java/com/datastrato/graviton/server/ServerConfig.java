/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class ServerConfig extends Config {

  public static final ConfigEntry<String> WEBSERVER_HOST =
      new ConfigBuilder("graviton.server.webserver.host")
          .doc("The host name of the built-in web server")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("0.0.0.0");

  public static final ConfigEntry<Integer> WEBSERVER_HTTP_PORT =
      new ConfigBuilder("graviton.server.webserver.httpPort")
          .doc("The http port number of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(8090);

  public static final ConfigEntry<Integer> WEBSERVER_CORE_THREADS =
      new ConfigBuilder("graviton.server.webserver.coreThreads")
          .doc("The core thread size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100));

  public static final ConfigEntry<Integer> WEBSERVER_MAX_THREADS =
      new ConfigBuilder("graviton.server.webserver.maxThreads")
          .doc("The max thread size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(Math.max(Runtime.getRuntime().availableProcessors() * 4, 400));

  public static final ConfigEntry<Long> WEBSERVER_STOP_IDLE_TIMEOUT =
      new ConfigBuilder("graviton.server.webserver.stopIdleTimeout")
          .doc("The stop idle timeout of the built-in web server")
          .version("0.1.0")
          .longConf()
          .createWithDefault(30 * 1000L);

  public static final ConfigEntry<Integer> WEBSERVER_REQUEST_HEADER_SIZE =
      new ConfigBuilder("graviton.server.webserver.requestHeaderSize")
          .doc("The request header size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(128 * 1024);

  public static final ConfigEntry<Integer> WEBSERVER_RESPONSE_HEADER_SIZE =
      new ConfigBuilder("graviton.server.webserver.responseHeaderSize")
          .doc("The response header size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(128 * 1024);

  public static final ConfigEntry<Integer> WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE =
      new ConfigBuilder("graviton.server.webserver.threadPoolWorkQueueSize")
          .doc("The executor thread pool work queue size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(100);

  public static final ConfigEntry<Integer> SERVER_SHUTDOWN_TIMEOUT =
      new ConfigBuilder("graviton.server.shutdown.timeout")
          .doc("The stop idle timeout(millis) of the Graviton Server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(3 * 1000);

  public ServerConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  public ServerConfig() {
    this(true);
  }
}
