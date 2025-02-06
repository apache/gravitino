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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.AbstractCatalogStore;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogStore is used to store catalog information to Apache Gravitino server. */
public class GravitinoCatalogStore extends AbstractCatalogStore {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogStore.class);
  private final GravitinoCatalogManager gravitinoCatalogManager;

  public GravitinoCatalogStore(GravitinoCatalogManager catalogManager) {
    this.gravitinoCatalogManager = catalogManager;
  }

  @Override
  public void storeCatalog(String catalogName, CatalogDescriptor descriptor)
      throws CatalogException {
    Configuration configuration = descriptor.getConfiguration();
    Map<String, String> gravitino = configuration.toMap();
    BaseCatalogFactory catalogFactory = getCatalogFactory(gravitino);
    Map<String, String> gravitinoProperties =
        catalogFactory.propertiesConverter().toGravitinoCatalogProperties(configuration);
    gravitinoCatalogManager.createCatalog(
        catalogName,
        catalogFactory.gravitinoCatalogType(),
        null,
        catalogFactory.gravitinoCatalogProvider(),
        gravitinoProperties);
  }

  @Override
  public void removeCatalog(String catalogName, boolean ignoreIfNotExists) throws CatalogException {
    try {
      gravitinoCatalogManager.dropCatalog(catalogName);
    } catch (Exception e) {
      throw new CatalogException(String.format("Failed to remove the catalog: %s", catalogName), e);
    }
  }

  @Override
  public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
    try {
      Catalog catalog = gravitinoCatalogManager.getGravitinoCatalogInfo(catalogName);
      BaseCatalogFactory catalogFactory = getCatalogFactory(catalog.provider());
      PropertiesConverter propertiesConverter = catalogFactory.propertiesConverter();
      Map<String, String> flinkCatalogProperties =
          propertiesConverter.toFlinkCatalogProperties(catalog.properties());
      CatalogDescriptor descriptor =
          CatalogDescriptor.of(catalogName, Configuration.fromMap(flinkCatalogProperties));
      return Optional.of(descriptor);
    } catch (Exception e) {
      LOG.warn("Failed to get the catalog:{}", catalogName, e);
      return Optional.empty();
    }
  }

  @Override
  public Set<String> listCatalogs() throws CatalogException {
    try {
      return gravitinoCatalogManager.listCatalogs();
    } catch (Exception e) {
      throw new CatalogException("Failed to list catalog.", e);
    }
  }

  @Override
  public boolean contains(String catalogName) throws CatalogException {
    return gravitinoCatalogManager.contains(catalogName);
  }

  private BaseCatalogFactory getCatalogFactory(Map<String, String> configuration) {
    String catalogType =
        Preconditions.checkNotNull(
            configuration.get(CommonCatalogOptions.CATALOG_TYPE.key()),
            "%s should not be null.",
            CommonCatalogOptions.CATALOG_TYPE);

    return discoverFactories(
        catalogFactory -> (catalogFactory.factoryIdentifier().equalsIgnoreCase(catalogType)),
        String.format(
            "Flink catalog type [%s] matched multiple flink catalog factories, it should only match one.",
            catalogType));
  }

  private BaseCatalogFactory getCatalogFactory(String provider) {
    return discoverFactories(
        catalogFactory ->
            ((BaseCatalogFactory) catalogFactory)
                .gravitinoCatalogProvider()
                .equalsIgnoreCase(provider),
        String.format(
            "Gravitino catalog provider [%s] matched multiple flink catalog factories, it should only match one.",
            provider));
  }

  private BaseCatalogFactory discoverFactories(Predicate<Factory> predicate, String errorMessage) {
    Iterator<Factory> serviceLoaderIterator = ServiceLoader.load(Factory.class).iterator();
    final List<Factory> factories = new ArrayList<>();
    while (true) {
      try {
        if (!serviceLoaderIterator.hasNext()) {
          break;
        }
        Factory catalogFactory = serviceLoaderIterator.next();
        if (catalogFactory instanceof BaseCatalogFactory && predicate.test(catalogFactory)) {
          factories.add(catalogFactory);
        }
      } catch (Throwable t) {
        if (t instanceof NoClassDefFoundError) {
          LOG.debug(
              "NoClassDefFoundError when loading a " + Factory.class.getCanonicalName() + ".", t);
        } else {
          throw new RuntimeException("Unexpected error when trying to load service provider.", t);
        }
      }
    }

    if (factories.isEmpty()) {
      throw new RuntimeException("Failed to correctly match the Flink catalog factory.");
    }
    // It should only match one.
    if (factories.size() > 1) {
      throw new RuntimeException(errorMessage);
    }
    return (BaseCatalogFactory) factories.get(0);
  }
}
