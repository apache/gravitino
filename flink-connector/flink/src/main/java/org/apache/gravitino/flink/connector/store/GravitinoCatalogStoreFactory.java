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

package org.apache.gravitino.flink.connector.store;

import static org.apache.flink.table.factories.FactoryUtil.createCatalogStoreFactoryHelper;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_ALLOW_THIRD_PARTY_CONNECTOR_LIST_CONFIG;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_CLIENT_CONFIG;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_METALAKE;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_URI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;

/** The Factory for creating {@link GravitinoCatalogStore}. */
public class GravitinoCatalogStoreFactory implements CatalogStoreFactory {

  private GravitinoCatalogManager catalogManager;
  private GenericInMemoryCatalogStore memoryCatalogStore;
  private List<String> allowThirdPartyConnectors;

  @Override
  public CatalogStore createCatalogStore() {
    return new GravitinoCatalogStore(catalogManager, memoryCatalogStore, allowThirdPartyConnectors);
  }

  @Override
  public void open(Context context) throws CatalogException {
    this.memoryCatalogStore = new GenericInMemoryCatalogStore();
    this.memoryCatalogStore.open();

    FactoryUtil.FactoryHelper<CatalogStoreFactory> factoryHelper =
        createCatalogStoreFactoryHelper(this, context);
    factoryHelper.validate();

    ReadableConfig options = factoryHelper.getOptions();

    String gravitinoUri = options.get(GRAVITINO_URI);
    String gravitinoName = options.get(GRAVITINO_METALAKE);
    Preconditions.checkArgument(
        gravitinoUri != null && gravitinoName != null,
        "Both %s and %s must be set",
        GRAVITINO_URI.key(),
        GRAVITINO_METALAKE.key());
    allowThirdPartyConnectors =
        Arrays.asList(
                Optional.ofNullable(options.get(GRAVITINO_ALLOW_THIRD_PARTY_CONNECTOR_LIST_CONFIG))
                    .map(s -> s.split(","))
                    .orElse(new String[] {}))
            .stream()
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());

    Preconditions.checkArgument(
        allowThirdPartyConnectors.stream().noneMatch(FactoryUtils.gravitinoFactoryList::contains),
        "The allowed third party connectors %s should not contain Gravitino connectors %s.",
        allowThirdPartyConnectors,
        FactoryUtils.gravitinoFactoryList);

    this.catalogManager =
        GravitinoCatalogManager.create(gravitinoUri, gravitinoName, extractClientConfig(options));
  }

  @Override
  public void close() throws CatalogException {
    if (memoryCatalogStore != null) {
      memoryCatalogStore.close();
    }
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  @Override
  public String factoryIdentifier() {
    return GRAVITINO;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(GRAVITINO_METALAKE, GRAVITINO_URI);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        GRAVITINO_CLIENT_CONFIG, GRAVITINO_ALLOW_THIRD_PARTY_CONNECTOR_LIST_CONFIG);
  }

  @VisibleForTesting
  static Map<String, String> extractClientConfig(ReadableConfig options) {
    return options.get(GRAVITINO_CLIENT_CONFIG).entrySet().stream()
        .collect(
            Collectors.toMap(
                entry ->
                    GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + entry.getKey(),
                Map.Entry::getValue,
                (oldVal, newVal) -> newVal));
  }
}
