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

import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers {@link SparkCatalogExtension}s on the classpath and merges them into a {@link
 * SparkBindings.Builder}: an entry that cannot be loaded, or that this build already binds a class
 * for, is logged and skipped rather than failing the whole build.
 */
final class SparkCatalogExtensionLoader {

  private static final Logger LOG = LoggerFactory.getLogger(SparkCatalogExtensionLoader.class);

  private SparkCatalogExtensionLoader() {}

  /**
   * Registers every discoverable {@link SparkCatalogExtension} into {@code builder} that this build
   * did not already bind at compile time.
   *
   * @param builder the builder to register discovered extensions into
   */
  static void registerDiscoveredCatalogs(SparkBindings.Builder builder) {
    registerDiscoveredCatalogs(builder, ServiceLoader.load(SparkCatalogExtension.class).iterator());
  }

  @VisibleForTesting
  static void registerDiscoveredCatalogs(
      SparkBindings.Builder builder, Iterator<SparkCatalogExtension> extensions) {
    while (true) {
      try {
        if (!extensions.hasNext()) {
          return;
        }
        registerOne(builder, extensions.next());
      } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
        // A provider that cannot be instantiated cannot report which provider it was for, so the
        // most useful thing left to log is that one entry was skipped.
        LOG.warn(
            "Skip a {} entry that could not be loaded.", SparkCatalogExtension.class.getName(), e);
      }
    }
  }

  private static void registerOne(SparkBindings.Builder builder, SparkCatalogExtension extension) {
    String provider = extension.provider();
    SparkCatalogKind kind = SparkCatalogKind.fromProvider(provider);
    if (kind == null) {
      LOG.warn(
          "Skip {} because provider {} is not supported by this connector.",
          extension.getClass().getName(),
          provider);
      return;
    }
    try {
      builder.catalog(kind, extension.catalogClassName());
    } catch (RuntimeException e) {
      LOG.warn(
          "Skip {} for provider {}: {}", extension.getClass().getName(), provider, e.getMessage());
    }
  }
}
