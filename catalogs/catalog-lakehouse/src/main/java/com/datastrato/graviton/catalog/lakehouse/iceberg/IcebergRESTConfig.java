/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class IcebergRESTConfig extends Config {
  public static final ConfigEntry<Integer> ICEBERG_REST_SERVER_HTTP_PORT =
      new ConfigBuilder("serverPort")
          .doc("The http port number of the Iceberg REST server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(9001);

  public static final ConfigEntry<String> ICEBERG_REST_SERVER_HOST =
      new ConfigBuilder("serverHost")
          .doc("The host name of the Iceberg REST server")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("0.0.0.0");

  public static final ConfigEntry<Integer> ICEBERG_REST_SERVER_CORE_THREADS =
      new ConfigBuilder("coreThreads")
          .doc("The core thread size of the Iceberg REST server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100));

  public static final ConfigEntry<Integer> ICEBERG_REST_SERVER_MAX_THREADS =
      new ConfigBuilder("maxThreads")
          .doc("The max thread size of the Iceberg REST server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(Math.max(Runtime.getRuntime().availableProcessors() * 4, 400));

  public static final ConfigEntry<Long> ICEBERG_REST_SERVER_STOP_IDLE_TIMEOUT =
      new ConfigBuilder("stopIdleTimeout")
          .doc("The stop idle timeout of the Iceberg REST server")
          .version("0.1.0")
          .longConf()
          .createWithDefault(30 * 1000L);

  public static final ConfigEntry<Integer> ICEBERG_REST_SERVER_THREAD_POOL_WORK_QUEUE_SIZE =
      new ConfigBuilder("threadPoolWorkQueueSize")
          .doc("The executor thread pool work queue size of the Iceberg REST server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(100);

  public static final ConfigEntry<Integer> ICEBERG_REST_SERVER_SHUTDOWN_TIMEOUT =
      new ConfigBuilder("shutdownTimeout")
          .doc("The stop idle timeout(millis) of the Iceberg REST Server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(3 * 1000);
}
