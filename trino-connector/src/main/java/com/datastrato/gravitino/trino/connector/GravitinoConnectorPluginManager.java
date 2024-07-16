/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoConfig.TRINO_PLUGIN_BUNDLES;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.resolver.ArtifactResolver;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.artifact.Artifact;

/** This class is mange the internal connector plugin and help to create the connector. */
public class GravitinoConnectorPluginManager {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoConnectorPluginManager.class);

  public static final String APP_CLASS_LOADER_NAME = "app";

  public static final String CONNECTOR_HIVE = "hive";
  public static final String CONNECTOR_ICEBERG = "iceberg";
  public static final String CONNECTOR_MYSQL = "mysql";
  public static final String CONNECTOR_POSTGRESQL = "postgresql";
  public static final String CONNECTOR_MEMORY = "memory";
  public static final String CONNECTOR_CLUSTER = "cluster";

  private static final String PLUGIN_NAME_PREFIX = "gravitino-";
  private static final String PLUGIN_CLASSLOADER_CLASS_NAME = "io.trino.server.PluginClassLoader";

  private static volatile GravitinoConnectorPluginManager instance;

  private Class<?> pluginLoaderClass;

  private static final Set<String> usePlugins =
      Set.of(
          CONNECTOR_HIVE,
          CONNECTOR_ICEBERG,
          CONNECTOR_MYSQL,
          CONNECTOR_POSTGRESQL,
          CONNECTOR_MEMORY,
          CONNECTOR_CLUSTER);

  private final Map<String, Plugin> connectorPlugins = new HashMap<>();
  private final ClassLoader appClassloader;

  public GravitinoConnectorPluginManager(ClassLoader classLoader) {
    this.appClassloader = classLoader;

    try {
      pluginLoaderClass = appClassloader.loadClass(PLUGIN_CLASSLOADER_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Can not load Plugin class loader", e);
    }

    if (GravitinoConfig.trinoConfig.contains(TRINO_PLUGIN_BUNDLES)) {
      loadPluginsFromBundle();
    } else {
      loadPluginsFromFile();
    }
  }

  public static GravitinoConnectorPluginManager instance(ClassLoader classLoader) {
    if (instance != null) {
      return instance;
    }
    synchronized (GravitinoConnectorPluginManager.class) {
      if (instance == null) {
        if (!APP_CLASS_LOADER_NAME.equals(classLoader.getName())) {
          throw new TrinoException(
              GRAVITINO_RUNTIME_ERROR,
              "Can not initialize GravitinoConnectorPluginManager when classLoader is not appClassLoader");
        }
        instance = new GravitinoConnectorPluginManager(classLoader);
      }
      return instance;
    }
  }

  public static GravitinoConnectorPluginManager instance() {
    if (instance == null) {
      throw new IllegalStateException("Need to call the function instance(ClassLoader) first");
    }
    return instance;
  }

  private void loadPluginsFromFile() {
    try {
      // Retrieve plugin directory
      // The Trino plugin director like:
      //    /data/trino/plugin/hive/**.jar
      //    /data/trino/plugin/gravitino/**.jar
      //    /data/trino/plugin/mysql/**.jar
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
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Error while loading plugins from file", e);
    }
  }

  private void loadPlugin(String pluginPath, String pluginName) {
    String dirName = pluginPath + "/" + pluginName;
    File directory = new File(dirName);
    if (!directory.exists()) {
      LOG.warn("Can not found plugin {} in directory {}", pluginName, dirName);
      return;
    }

    File[] pluginFiles = directory.listFiles();
    if (pluginFiles == null || pluginFiles.length == 0) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Can not found any files plugin directory " + dirName);
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
    loadPluginWithUrls(files, pluginName);
  }

  private void loadPluginWithUrls(List<URL> urls, String pluginName) {
    try {
      Constructor<?> constructor =
          pluginLoaderClass.getConstructor(String.class, List.class, ClassLoader.class, List.class);
      // The classloader name will use to serialize the Handle object
      String classLoaderName = PLUGIN_NAME_PREFIX + pluginName;
      // Load Trino SPI package and other dependencies refer to io.trino.server.PluginClassLoader
      Object pluginClassLoader =
          constructor.newInstance(
              classLoaderName,
              urls,
              appClassloader,
              List.of(
                  "io.trino.spi.",
                  "com.fasterxml.jackson.annotation.",
                  "io.airlift.slice.",
                  "org.openjdk.jol.",
                  "io.opentelemetry.api.",
                  "io.opentelemetry.context."));

      ServiceLoader<Plugin> serviceLoader =
          ServiceLoader.load(Plugin.class, (ClassLoader) pluginClassLoader);
      List<Plugin> pluginList = ImmutableList.copyOf(serviceLoader);
      if (pluginList.isEmpty()) {
        throw new TrinoException(
            GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR,
            String.format("The %s plugin does not found connector SIP interface", pluginName));
      }
      Plugin plugin = pluginList.get(0);
      if (plugin.getConnectorFactories() == null
          || !plugin.getConnectorFactories().iterator().hasNext()) {
        throw new TrinoException(
            GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR,
            String.format("The %s plugin does not contains any ConnectorFactories", pluginName));
      }
      connectorPlugins.put(pluginName, pluginList.get(0));

    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Failed to create Plugin class loader " + pluginName, e);
    }
  }

  private void loadPluginsFromBundle() {
    // load plugin from bundle config
    // plugin.bundles=\
    //  ../../plugin/trino-jmx/pom.xml,\
    //  ../../plugin/trino-hive/pom.xml,\

    ArtifactResolver artifactResolver =
        new ArtifactResolver(ArtifactResolver.USER_LOCAL_REPO, ArtifactResolver.MAVEN_CENTRAL_URI);

    String value = GravitinoConfig.trinoConfig.getProperty(TRINO_PLUGIN_BUNDLES);
    Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
    splitter.splitToList(value).stream()
        .forEach(
            v -> {
              int start = v.indexOf("trino-");
              if (start == -1) {
                return;
              }
              int end = v.indexOf('/', start);
              if (end == -1) {
                return;
              }
              String key = v.substring(start, end).replace("trino-", "");
              if (!usePlugins.contains(key)) {
                return;
              }
              try {
                loadPluginByPom(artifactResolver.resolvePom(new File(v)), key);
              } catch (Throwable t) {
                LOG.error("Fatal error in load plugin by {}", v, t);
              }
            });
  }

  private void loadPluginByPom(List<Artifact> artifacts, String pluginName) {
    try {
      List<URL> urls = new ArrayList<>();
      for (Artifact artifact : artifacts) {
        if (artifact.getFile() == null) {
          throw new RuntimeException("Could not resolve artifact: " + artifact);
        }
        File file = artifact.getFile().getCanonicalFile();
        urls.add(file.toURI().toURL());
      }
      File root =
          new File(
              artifacts.get(0).getFile().getParentFile().getCanonicalFile(), "plugin-discovery");
      urls.add(root.toURI().toURL());
      loadPluginWithUrls(urls, pluginName);
    } catch (Exception e) {
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Error while loading plugins from pom", e);
    }
  }

  public void installPlugin(String pluginName, Plugin plugin) {
    connectorPlugins.put(pluginName, plugin);
  }

  public Connector createConnector(
      String connectorName, Map<String, String> config, ConnectorContext context) {
    try {
      Plugin plugin = connectorPlugins.get(connectorName);
      if (plugin == null) {
        throw new TrinoException(
            GRAVITINO_RUNTIME_ERROR, "Can not found plugin for connector " + connectorName);
      }
      try (ThreadContextClassLoader ignored =
          new ThreadContextClassLoader(plugin.getClass().getClassLoader())) {
        ConnectorFactory connectorFactory = plugin.getConnectorFactories().iterator().next();
        Connector connector = connectorFactory.create(connectorName, config, context);
        LOG.info("create connector {} with config {} successful", connectorName, config);
        return connector;
      }
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Failed to create connector " + connectorName, e);
    }
  }

  public ClassLoader getClassLoader(String classLoaderName) {
    if (classLoaderName.equals(APP_CLASS_LOADER_NAME)) {
      return appClassloader;
    }

    Plugin plugin = connectorPlugins.get(classLoaderName.substring(PLUGIN_NAME_PREFIX.length()));
    if (plugin == null) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Can not found class loader for " + classLoaderName);
    }
    return plugin.getClass().getClassLoader();
  }

  public ClassLoader getAppClassloader() {
    return appClassloader;
  }
}
