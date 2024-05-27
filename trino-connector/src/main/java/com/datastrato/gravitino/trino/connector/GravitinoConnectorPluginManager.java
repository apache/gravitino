/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_OPERATION_FAILED;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is mange the internal connector plugin and help to create the connector. */
public class GravitinoConnectorPluginManager {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoConnectorFactory.class);

  public static final String CONNECTOR_HIVE = "hive";
  public static final String CONNECTOR_ICEBERG = "iceberg";
  public static final String CONNECTOR_MYSQL = "mysql";
  public static final String CONNECTOR_POSTGRESQL = "postgresql";
  public static final String CONNECTOR_MEMORY = "memory";

  private static final String PLUGIN_NAME_PREFIX = "gravitino-";
  private static final String PLUGIN_CLASSLOADER_CLASS_NAME = "io.trino.server.PluginClassLoader";

  private static volatile GravitinoConnectorPluginManager instance;

  private final Class<?> pluginLoaderClass;

  private static final Set<String> usePlugins =
      Set.of(
          CONNECTOR_HIVE,
          CONNECTOR_ICEBERG,
          CONNECTOR_MYSQL,
          CONNECTOR_POSTGRESQL,
          CONNECTOR_MEMORY);

  private final Map<String, ClassLoader> pluginClassLoaders = new HashMap<>();
  private final ClassLoader appClassloader;

  public GravitinoConnectorPluginManager(ClassLoader classLoader) {
    try {
      // Retrieve plugin directory
      this.appClassloader = classLoader;
      pluginLoaderClass = appClassloader.loadClass(PLUGIN_CLASSLOADER_CLASS_NAME);
      String jarPath =
          GravitinoConnectorPluginManager.class
              .getProtectionDomain()
              .getCodeSource()
              .getLocation()
              .toURI()
              .getPath();
      String pluginDir = Paths.get(jarPath).getParent().getParent().toString();

      // Load all plugins
      for (String pluginName : usePlugins) {
        loadPlugin(pluginDir, pluginName);
        LOG.info("Load plugin {}/{} successful", pluginDir, pluginName);
      }
    } catch (Exception e) {
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Error while loading plugins", e);
    }
  }

  public static GravitinoConnectorPluginManager instance(ClassLoader classLoader) {
    if (instance != null) {
      return instance;
    }
    synchronized (GravitinoConnectorPluginManager.class) {
      if (instance == null) {
        instance = new GravitinoConnectorPluginManager(classLoader);
      }
      return instance;
    }
  }

  private void loadPlugin(String pluginPath, String pluginName) {
    String dirName = pluginPath + "." + pluginName;
    File directory = new File(dirName);
    if (!directory.exists()) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Can not found plugin directory " + pluginPath);
    }

    File[] pluginFiles = directory.listFiles();
    if (pluginFiles == null || pluginFiles.length == 0) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Can not found any files plugin directory " + pluginPath);
    }
    List<URL> files =
        Arrays.stream(pluginFiles)
            .map(File::toURI)
            .map(
                uri -> {
                  try {
                    return uri.toURL();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .toList();

    try {
      Constructor<?> constructor =
          pluginLoaderClass.getConstructor(String.class, List.class, ClassLoader.class, List.class);
      String classLoaderName = PLUGIN_NAME_PREFIX + pluginName;
      Object pluginClassLoader =
          constructor.newInstance(
              classLoaderName,
              files,
              appClassloader,
              List.of(
                  "io.trino.spi.",
                  "com.fasterxml.jackson.annotation.",
                  "io.airlift.slice.",
                  "org.openjdk.jol.",
                  "io.opentelemetry.api.",
                  "io.opentelemetry.context."));
      pluginClassLoaders.put(pluginName, (ClassLoader) pluginClassLoader);
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Failed to create Plugin class loader " + pluginName, e);
    }
  }

  public Connector createConnector(
      String connectorName, Map<String, String> config, ConnectorContext context) {
    try {
      ClassLoader pluginClassLoader = pluginClassLoaders.get(connectorName);
      if (pluginClassLoader == null) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED,
            "Gravitino connector does not support connector " + connectorName);
      }

      ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
      List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
      if (plugins.isEmpty()) {
        throw new TrinoException(
            GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR,
            String.format("The %s plugin does not found connector SIP interface", connectorName));
      }
      Plugin plugin = plugins.get(0);

      try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
        if (plugin.getConnectorFactories() == null
            || !plugin.getConnectorFactories().iterator().hasNext()) {
          throw new TrinoException(
              GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR,
              String.format(
                  "The %s plugin does not contains any ConnectorFactories", connectorName));
        }
        ConnectorFactory connectorFactory = plugin.getConnectorFactories().iterator().next();
        Connector connector = connectorFactory.create(connectorName, config, context);
        LOG.info("create connector {} with config {} successful", connectorName, config);
        return connector;
      } catch (Exception e) {
        throw new TrinoException(
            GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR, "Failed to create internal connector", e);
      }
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Failed to create connector " + connectorName, e);
    }
  }
}
