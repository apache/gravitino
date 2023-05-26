package com.datastrato.unified_catalog.server;

import com.datastrato.unified_catalog.config.ConfigBuilder;
import com.datastrato.unified_catalog.config.ConfigEntry;

public class ServerConfig {

  public static final ConfigEntry<Integer> WEBSERVER_PORT =
      new ConfigBuilder("unified-catalog.server.webserver.port")
          .doc("The port number of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(8090);

  public static final ConfigEntry<Integer> WEBSERVER_CORE_THREADS =
      new ConfigBuilder("unified-catalog.server.webserver.coreThreads")
          .doc("The core thread size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(
              Math.min(Runtime.getRuntime().availableProcessors() * 2, 100));

  public static final ConfigEntry<Integer> WEBSERVER_MAX_THREADS =
      new ConfigBuilder("unified-catalog.server.webserver.maxThreads")
          .doc("The max thread size of the built-in web server")
          .version("0.1.0")
          .intConf()
          .createWithDefault(
              Math.max(Runtime.getRuntime().availableProcessors() * 4, 400));

  public static final ConfigEntry<Long> WEBSERVER_STOP_IDLE_TIMEOUT =
      new ConfigBuilder("unified-catalog.server.webserver.stopIdleTimeout")
          .doc("The stop idle timeout of the built-in web server")
          .version("0.1.0")
          .longConf()
          .createWithDefault(30* 1000L);







}
