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

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;

public class GravitinoIcebergCatalogFactory implements BaseCatalogFactory {

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    return new GravitinoIcebergCatalog(
        context.getName(),
        helper.getOptions().get(GravitinoIcebergCatalogFactoryOptions.DEFAULT_DATABASE),
        propertiesConverter(),
        partitionConverter(),
        context.getOptions());
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

  /**
   * Define properties converter.
   *
   * @return The properties converter instance for Iceberg catalog.
   */
  @Override
  public PropertiesConverter propertiesConverter() {
    return IcebergPropertiesConverter.INSTANCE;
  }

  @Override
  public PartitionConverter partitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }
}
