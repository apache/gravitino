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
package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;
import org.apache.iceberg.rest.auth.AuthProperties;

public class GravitinoIcebergCatalogFactory implements BaseCatalogFactory {

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    return newCatalog(
        context.getName(),
        helper.getOptions().get(GravitinoIcebergCatalogFactoryOptions.DEFAULT_DATABASE),
        schemaAndTablePropertiesConverter(),
        partitionConverter(),
        context.getOptions(),
        toIcebergCatalogOptions(context.getOptions()));
  }

  protected Catalog newCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      Map<String, String> icebergCatalogProperties) {
    return new GravitinoIcebergCatalog(
        catalogName,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter,
        catalogOptions,
        icebergCatalogProperties);
  }

  @Override
  public String factoryIdentifier() {
    return GravitinoIcebergCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  /**
   * Define gravitino catalog provider.
   *
   * @return The name of the Gravitino catalog provider, which is "lakehouse-iceberg" for this
   *     implementation.
   */
  @Override
  public String gravitinoCatalogProvider() {
    return "lakehouse-iceberg";
  }

  /**
   * Define gravitino catalog type.
   *
   * @return The type of the Gravitino catalog, which is RELATIONAL for this implementation.
   */
  @Override
  public org.apache.gravitino.Catalog.Type gravitinoCatalogType() {
    return org.apache.gravitino.Catalog.Type.RELATIONAL;
  }

  @Override
  public CatalogPropertiesConverter catalogPropertiesConverter() {
    return IcebergPropertiesConverter.INSTANCE;
  }

  public SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter() {
    return IcebergPropertiesConverter.INSTANCE;
  }

  @Override
  public PartitionConverter partitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }

  @VisibleForTesting
  Map<String, String> toIcebergCatalogOptions(Map<String, String> catalogOptions) {
    Map<String, String> icebergCatalogOptions = Maps.newHashMap(catalogOptions);
    String catalogBackend =
        catalogOptions.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND);
    // catalogBackend is only present here on the CREATE CATALOG path (raw user SQL options). On
    // the USE CATALOG path (loading a catalog already persisted in Gravitino),
    // IcebergPropertiesConverter has already renamed catalog-backend -> catalog-type during
    // property conversion, so catalogBackend is null even for a REST/JDBC catalog. This
    // normalizes the CREATE CATALOG case so both paths converge on the same catalog-type key.
    String catalogType = normalizeCatalogType(icebergCatalogOptions, catalogBackend);
    // catalogType (not catalogBackend, which is unreliable as explained above) must be used for
    // every backend check from here on, or it silently no-ops on the USE CATALOG path -- this was
    // the root cause of #11601 (REST auth never propagated when loading a persisted catalog).
    propagateRestAuthIfNeeded(icebergCatalogOptions, catalogType);
    translateJdbcBackendToCatalogImpl(icebergCatalogOptions, catalogType);
    // The outer Flink factory is `gravitino-iceberg`, but the nested Iceberg factory still expects
    // `catalog-type=iceberg` when building the native Iceberg catalog instance.
    icebergCatalogOptions.put(CommonCatalogOptions.CATALOG_TYPE.key(), "iceberg");
    return icebergCatalogOptions;
  }

  // Copies catalog-backend into catalog-type so the CREATE CATALOG path lines up with the
  // USE CATALOG path, where the rename already happened upstream. Skipped when catalog-type or
  // catalog-impl is already set, otherwise an explicitly provided catalog-impl would conflict
  // with it. Returns the effective backend type for downstream gating: the normalized catalog-type
  // when present, or catalog-backend as a fallback (e.g. when catalog-impl was already set).
  private static String normalizeCatalogType(
      Map<String, String> icebergCatalogOptions, String catalogBackend) {
    if (catalogBackend != null
        && !icebergCatalogOptions.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE)
        && !icebergCatalogOptions.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL)) {
      icebergCatalogOptions.put(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, catalogBackend);
    }
    return icebergCatalogOptions.getOrDefault(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, catalogBackend);
  }

  // A REST backend connects directly to the Iceberg REST service, bypassing the Gravitino
  // server's auth proxy, so the Gravitino client's authentication must be propagated to the REST
  // client, unless the user has already configured REST auth explicitly.
  private static void propagateRestAuthIfNeeded(
      Map<String, String> icebergCatalogOptions, String catalogType) {
    if (IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST.equalsIgnoreCase(catalogType)
        && !icebergCatalogOptions.containsKey(AuthProperties.AUTH_TYPE)) {
      icebergCatalogOptions.putAll(
          IcebergPropertiesConverter.INSTANCE.toRestAuthProperties(
              GravitinoCatalogManager.get().getGravitinoClientConfig()));
    }
  }

  // Iceberg's FlinkCatalogFactory only accepts hive/hadoop/rest as catalog-type; a JDBC backend
  // must be loaded through catalog-impl instead. The two keys are mutually exclusive, so
  // catalog-type is dropped, and putIfAbsent respects an explicitly provided catalog-impl.
  private static void translateJdbcBackendToCatalogImpl(
      Map<String, String> icebergCatalogOptions, String catalogType) {
    if (IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC.equalsIgnoreCase(catalogType)) {
      icebergCatalogOptions.remove(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE);
      icebergCatalogOptions.putIfAbsent(
          IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL,
          IcebergPropertiesConstants.ICEBERG_JDBC_CATALOG_IMPL);
    }
  }
}
