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

package org.apache.gravitino.auxiliary;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuxiliaryServiceManager manage all GravitinoAuxiliaryServices with isolated classloader provided
 */
public class AuxiliaryServiceManager {

  private static final Logger LOG = LoggerFactory.getLogger(AuxiliaryServiceManager.class);
  public static final String GRAVITINO_AUX_SERVICE_PREFIX = "gravitino.auxService.";
  public static final String AUX_SERVICE_NAMES = "names";
  public static final String AUX_SERVICE_CLASSPATH = "classpath";

  private static final Splitter splitter = Splitter.on(",");
  private static final Joiner DOT = Joiner.on(".");

  private Map<String, GravitinoAuxiliaryService> auxServices = new HashMap<>();
  private Map<String, IsolatedClassLoader> auxServiceClassLoaders = new HashMap<>();

  private Exception firstException;

  private Class<? extends GravitinoAuxiliaryService> lookupAuxService(
      String provider, ClassLoader cl) {
    ServiceLoader<GravitinoAuxiliaryService> loader =
        ServiceLoader.load(GravitinoAuxiliaryService.class, cl);
    List<Class<? extends GravitinoAuxiliaryService>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(GravitinoAuxiliaryService::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No GravitinoAuxiliaryService found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple GravitinoAuxiliaryService found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }

  @VisibleForTesting
  public GravitinoAuxiliaryService loadAuxService(
      String auxServiceName, IsolatedClassLoader isolatedClassLoader) throws Exception {
    return isolatedClassLoader.withClassLoader(
        cl -> {
          try {
            Class<? extends GravitinoAuxiliaryService> providerClz =
                lookupAuxService(auxServiceName, cl);
            return providerClz.getDeclaredConstructor().newInstance();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  @VisibleForTesting
  public IsolatedClassLoader getIsolatedClassLoader(List<String> classPaths) {
    return IsolatedClassLoader.buildClassLoader(classPaths);
  }

  @VisibleForTesting
  static String getValidPath(String auxServiceName, String pathString) {
    Path path = Paths.get(pathString);
    if (Files.exists(path)) {
      return path.toAbsolutePath().toString();
    }

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (!path.isAbsolute() && gravitinoHome != null) {
      Path newPath = Paths.get(gravitinoHome, pathString);
      if (Files.exists(newPath)) {
        return newPath.toString();
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "AuxService:%s, classpath: %s not exists, gravitinoHome:%s",
            auxServiceName, pathString, gravitinoHome));
  }

  private void registerAuxService(String auxServiceName, Map<String, String> config) {
    String classpath = config.get(AUX_SERVICE_CLASSPATH);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(classpath),
        String.format(
            "AuxService:%s, %s%s.%s is not set in configuration",
            auxServiceName, "gravitino.", auxServiceName, AUX_SERVICE_CLASSPATH));

    List<String> validPaths =
        splitter
            .trimResults()
            .omitEmptyStrings()
            .splitToStream(classpath)
            .map(path -> getValidPath(auxServiceName, path))
            .collect(Collectors.toList());
    LOG.info(
        "AuxService name:{}, config:{}, valid classpath:{}", auxServiceName, config, validPaths);

    IsolatedClassLoader isolatedClassLoader = getIsolatedClassLoader(validPaths);
    try {
      GravitinoAuxiliaryService gravitinoAuxiliaryService =
          loadAuxService(auxServiceName, isolatedClassLoader);
      auxServices.put(auxServiceName, gravitinoAuxiliaryService);
      auxServiceClassLoaders.put(auxServiceName, isolatedClassLoader);
    } catch (Exception e) {
      LOG.error("Failed to register auxService: {}", auxServiceName, e);
      throw new RuntimeException(e);
    }
    LOG.info("AuxService:{} registered successfully", auxServiceName);
  }

  private void registerAuxServices(Map<String, String> config) {
    String auxServiceNames = config.getOrDefault(AUX_SERVICE_NAMES, "");
    splitter
        .omitEmptyStrings()
        .trimResults()
        .splitToStream(auxServiceNames)
        .forEach(
            auxServiceName ->
                registerAuxService(
                    auxServiceName, MapUtils.getPrefixMap(config, DOT.join(auxServiceName, ""))));
  }

  private void doWithClassLoader(String auxServiceName, Consumer<IsolatedClassLoader> func) {
    IsolatedClassLoader classLoader = auxServiceClassLoaders.get(auxServiceName);
    try {
      classLoader.withClassLoader(
          cl -> {
            try {
              func.accept(classLoader);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return null;
          });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void serviceInit(Config gravitinoConfig) {
    Map<String, String> serviceConfigs = extractAuxiliaryServiceConfigs(gravitinoConfig);
    registerAuxServices(serviceConfigs);
    auxServices.forEach(
        (auxServiceName, auxService) -> {
          doWithClassLoader(
              auxServiceName,
              cl ->
                  auxService.serviceInit(
                      MapUtils.getPrefixMap(serviceConfigs, DOT.join(auxServiceName, ""))));
        });
  }

  public void serviceStart() {
    auxServices.forEach(
        (auxServiceName, auxService) -> {
          doWithClassLoader(auxServiceName, cl -> auxService.serviceStart());
        });
  }

  private void stopQuietly(String auxServiceName, GravitinoAuxiliaryService auxiliaryService) {
    try {
      auxiliaryService.serviceStop();
    } catch (Exception e) {
      LOG.warn("AuxService:{} stop failed", auxServiceName, e);
      if (firstException == null) {
        firstException = e;
      }
    }
  }

  public void serviceStop() throws Exception {
    auxServices.forEach(
        (auxServiceName, auxService) -> {
          doWithClassLoader(auxServiceName, cl -> stopQuietly(auxServiceName, auxService));
        });
    if (firstException != null) {
      throw firstException;
    }
  }

  // Extract aux service configs, transform gravitino.$serviceName.key to $serviceName.key.
  // And will transform gravitino.auxService.$serviceName.key to $serviceName.key to keep
  // compatibility.
  @VisibleForTesting
  static Map<String, String> extractAuxiliaryServiceConfigs(Config config) {
    String auxServiceNames =
        config
            .getConfigsWithPrefix(GRAVITINO_AUX_SERVICE_PREFIX)
            .getOrDefault(AUX_SERVICE_NAMES, "");
    Map<String, String> serviceConfigs = new HashMap<>();
    serviceConfigs.put(AUX_SERVICE_NAMES, auxServiceNames);
    config
        .getAllConfig()
        .forEach(
            (k, v) -> {
              if (k.startsWith(GRAVITINO_AUX_SERVICE_PREFIX)) {
                String extractKey = k.substring(GRAVITINO_AUX_SERVICE_PREFIX.length());
                LOG.warn(
                    "The configuration {} is deprecated(still working), please use gravitino.{} instead.",
                    k,
                    extractKey);
                serviceConfigs.put(extractKey, v);
              }
            });
    splitter
        .omitEmptyStrings()
        .trimResults()
        .splitToStream(auxServiceNames)
        .forEach(
            name ->
                config.getAllConfig().forEach((k, v) -> extractConfig(serviceConfigs, name, k, v)));
    return serviceConfigs;
  }

  private static void extractConfig(
      Map<String, String> serverConfig, String serverName, String configKey, String configValue) {
    if (configKey.startsWith(String.format("gravitino.%s.", serverName))) {
      String extractedKey = configKey.substring("gravitino.".length());
      String originValue = serverConfig.put(extractedKey, configValue);
      if (originValue != null) {
        LOG.warn(
            "The configuration {}{} is overwritten by {}",
            GRAVITINO_AUX_SERVICE_PREFIX,
            extractedKey,
            configKey);
      }
    }
  }
}
