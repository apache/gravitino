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
package org.apache.gravitino.spark.connector.plugin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves Spark catalog classes contributed through {@link SparkCatalogExtension}. */
public class SparkCatalogExtensions {

  private static final Logger LOG = LoggerFactory.getLogger(SparkCatalogExtensions.class);

  private static volatile Map<String, String> catalogClassNamesByProvider;

  private SparkCatalogExtensions() {}

  /**
   * Looks up the Spark catalog class an extension registered for a provider.
   *
   * @param provider the Gravitino catalog provider
   * @return the catalog class name, or null when no extension serves the provider
   */
  @Nullable
  public static String catalogClassName(String provider) {
    return extensions().get(provider.toLowerCase(Locale.ROOT));
  }

  private static Map<String, String> extensions() {
    Map<String, String> loaded = catalogClassNamesByProvider;
    if (loaded == null) {
      synchronized (SparkCatalogExtensions.class) {
        loaded = catalogClassNamesByProvider;
        if (loaded == null) {
          loaded = load();
          catalogClassNamesByProvider = loaded;
        }
      }
    }
    return loaded;
  }

  private static Map<String, String> load() {
    Map<String, String> byProvider = new HashMap<>();
    Iterator<SparkCatalogExtension> iterator =
        ServiceLoader.load(SparkCatalogExtension.class, extensionClassLoader()).iterator();
    while (true) {
      try {
        if (!iterator.hasNext()) {
          break;
        }
        SparkCatalogExtension extension = iterator.next();
        String provider = extension.provider();
        String catalogClassName = extension.catalogClassName();
        if (StringUtils.isBlank(provider) || StringUtils.isBlank(catalogClassName)) {
          LOG.error(
              "Skip Spark catalog extension {}: provider and catalog class name must not be blank,"
                  + " got provider={}, catalogClassName={}.",
              extension.getClass().getName(),
              provider,
              catalogClassName);
          continue;
        }
        String normalized = provider.toLowerCase(Locale.ROOT);
        String previous = byProvider.putIfAbsent(normalized, catalogClassName);
        if (previous != null) {
          LOG.warn(
              "Ignore Spark catalog extension {} for provider {}: already served by {}.",
              catalogClassName,
              normalized,
              previous);
        }
      } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
        // ServiceLoader reports a missing provider class, a malformed META-INF entry or a failing
        // constructor as ServiceConfigurationError; an extension built against another Spark
        // version fails with a LinkageError once its methods run, and a misconfigured one may
        // throw from provider() or catalogClassName(). Skip that entry and keep the rest.
        LOG.error("Skip a Spark catalog extension that cannot be loaded.", e);
      }
    }
    LOG.info("Discovered Spark catalog extensions: {}", byProvider);
    return byProvider;
  }

  /**
   * Prefers the context class loader so extension jars added through {@code --jars} are visible
   * when the connector itself sits on the driver class path.
   */
  private static ClassLoader extensionClassLoader() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return loader != null ? loader : SparkCatalogExtensions.class.getClassLoader();
  }
}
