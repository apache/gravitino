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
        ServiceLoader.load(
                SparkCatalogExtension.class, SparkCatalogExtensions.class.getClassLoader())
            .iterator();
    while (true) {
      try {
        if (!iterator.hasNext()) {
          break;
        }
        SparkCatalogExtension extension = iterator.next();
        String provider = extension.provider().toLowerCase(Locale.ROOT);
        String previous = byProvider.putIfAbsent(provider, extension.catalogClassName());
        if (previous != null) {
          LOG.warn(
              "Ignore Spark catalog extension {} for provider {}: already served by {}.",
              extension.catalogClassName(),
              provider,
              previous);
        }
      } catch (ServiceConfigurationError e) {
        // An extension jar built for another Spark version may fail to link; keep the rest.
        LOG.warn("Skip a Spark catalog extension that cannot be loaded.", e);
      }
    }
    return byProvider;
  }
}
