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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.AbstractCatalogStore;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.hive.HivePropertiesConverter;
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
    String provider = getGravitinoCatalogProvider(configuration);
    Catalog.Type type = getGravitinoCatalogType(configuration);
    Map<String, String> gravitinoProperties =
        getPropertiesConverter(provider).toGravitinoCatalogProperties(configuration);
    gravitinoCatalogManager.createCatalog(catalogName, type, null, provider, gravitinoProperties);
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
      String provider = catalog.provider();
      PropertiesConverter propertiesConverter = getPropertiesConverter(provider);
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

  private String getGravitinoCatalogProvider(Configuration configuration) {
    String catalogType =
        Preconditions.checkNotNull(
            configuration.get(CommonCatalogOptions.CATALOG_TYPE),
            "%s should not be null.",
            CommonCatalogOptions.CATALOG_TYPE);

    switch (catalogType) {
      case GravitinoHiveCatalogFactoryOptions.IDENTIFIER:
        return "hive";
      default:
        throw new IllegalArgumentException(
            String.format("The catalog type is not supported:%s", catalogType));
    }
  }

  private Catalog.Type getGravitinoCatalogType(Configuration configuration) {
    String catalogType =
        Preconditions.checkNotNull(
            configuration.get(CommonCatalogOptions.CATALOG_TYPE),
            "%s should not be null.",
            CommonCatalogOptions.CATALOG_TYPE);

    switch (catalogType) {
      case GravitinoHiveCatalogFactoryOptions.IDENTIFIER:
        return Catalog.Type.RELATIONAL;
      default:
        throw new IllegalArgumentException(
            String.format("The catalog type is not supported:%s", catalogType));
    }
  }

  private PropertiesConverter getPropertiesConverter(String provider) {
    switch (provider) {
      case "hive":
        return HivePropertiesConverter.INSTANCE;
    }
    throw new IllegalArgumentException("The provider is not supported:" + provider);
  }
}
