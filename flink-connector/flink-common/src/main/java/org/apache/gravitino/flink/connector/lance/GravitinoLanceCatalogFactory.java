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

package org.apache.gravitino.flink.connector.lance;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;

/** Factory for creating {@link GravitinoLanceCatalog} instances. */
public abstract class GravitinoLanceCatalogFactory implements BaseCatalogFactory {

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    return newCatalog(
        context.getName(),
        helper.getOptions().get(GravitinoLanceCatalogFactoryOptions.DEFAULT_DATABASE),
        schemaAndTablePropertiesConverter(),
        partitionConverter(),
        context.getOptions());
  }

  /**
   * Creates a new version-specific {@link GravitinoLanceCatalog} instance.
   *
   * @param catalogName the catalog name
   * @param defaultDatabase the default database name
   * @param schemaAndTablePropertiesConverter the properties converter
   * @param partitionConverter the partition converter
   * @param catalogOptions the catalog options
   * @return the created catalog instance
   */
  protected abstract Catalog newCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions);

  @Override
  public String factoryIdentifier() {
    return GravitinoLanceCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        GravitinoLanceCatalogFactoryOptions.DEFAULT_DATABASE,
        GravitinoLanceCatalogFactoryOptions.URI,
        GravitinoLanceCatalogFactoryOptions.CATALOG_BACKEND,
        GravitinoLanceCatalogFactoryOptions.IMPL,
        GravitinoLanceCatalogFactoryOptions.WAREHOUSE,
        GravitinoLanceCatalogFactoryOptions.LOCATION);
  }

  @Override
  public String gravitinoCatalogProvider() {
    return "lakehouse-generic";
  }

  @Override
  public boolean supportsProvider(String provider) {
    return "lakehouse-generic".equalsIgnoreCase(provider)
        || "lakehouse-lance".equalsIgnoreCase(provider)
        || "lance".equalsIgnoreCase(provider);
  }

  @Override
  public org.apache.gravitino.Catalog.Type gravitinoCatalogType() {
    return org.apache.gravitino.Catalog.Type.RELATIONAL;
  }

  @Override
  public CatalogPropertiesConverter catalogPropertiesConverter() {
    return LancePropertiesConverter.INSTANCE;
  }

  /**
   * Returns the schema and table properties converter for Lance.
   *
   * @return the schema and table properties converter
   */
  public SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter() {
    return LancePropertiesConverter.INSTANCE;
  }

  @Override
  public PartitionConverter partitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }
}
