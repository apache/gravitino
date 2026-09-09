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
package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A factory that discovers {@link TableLocationProvider}s through {@link ServiceLoader}. */
public class TableLocationProviderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TableLocationProviderFactory.class);

  private TableLocationProviderFactory() {}

  /**
   * Creates and initializes the {@link TableLocationProvider} registered under the given name.
   *
   * <p>A new instance is returned on every call, so the caller owns its lifecycle and is
   * responsible for closing it.
   *
   * @param name the provider name to look up, matched case-insensitively against {@link
   *     TableLocationProvider#name()}
   * @param catalogProperties the properties of the catalog the provider belongs to
   * @return the initialized provider
   * @throws IllegalArgumentException if no provider, or more than one provider, is registered under
   *     the given name
   */
  public static TableLocationProvider create(String name, Map<String, String> catalogProperties) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "Table location provider name must not be blank");

    ClassLoader cl =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(TableLocationProvider.class.getClassLoader());
    ServiceLoader<TableLocationProvider> loader =
        ServiceLoader.load(TableLocationProvider.class, cl);

    List<TableLocationProvider> providers =
        loader.stream()
            .map(ServiceLoader.Provider::get)
            .filter(provider -> name.equalsIgnoreCase(provider.name()))
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("No TableLocationProvider found for name '%s'", name));
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          String.format(
              "Multiple TableLocationProviders found for name '%s': %s",
              name,
              providers.stream()
                  .map(provider -> provider.getClass().getName())
                  .collect(Collectors.joining(", "))));
    }

    TableLocationProvider provider = providers.get(0);
    provider.initialize(catalogProperties);
    LOG.info("Loaded TableLocationProvider '{}': {}", name, provider.getClass().getName());
    return provider;
  }
}
