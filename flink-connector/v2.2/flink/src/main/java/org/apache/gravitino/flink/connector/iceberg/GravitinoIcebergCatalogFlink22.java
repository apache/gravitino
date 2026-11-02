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

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.flink.connector.utils.CatalogCompatFlink22;
import org.apache.iceberg.flink.FlinkCatalogFactory;

/** {@link GravitinoIcebergCatalog} implementation for Flink 2.2. */
public class GravitinoIcebergCatalogFlink22 extends GravitinoIcebergCatalog {

  /**
   * Creates a catalog backed by the Flink 2.2 connector.
   *
   * @param catalogName the catalog name
   * @param defaultDatabase the default database name
   * @param schemaAndTablePropertiesConverter the converter for schemas and table properties
   * @param partitionConverter the converter for partition specifications
   * @param catalogOptions the Gravitino catalog options
   * @param icebergCatalogProperties the properties of the underlying Iceberg catalog
   */
  protected GravitinoIcebergCatalogFlink22(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      Map<String, String> icebergCatalogProperties) {
    super(
        catalogName,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter,
        catalogOptions,
        icebergCatalogProperties);
  }

  /** {@inheritDoc} */
  @Override
  protected Object createInnerIcebergCatalog(String catalogName, Map<String, String> properties) {
    // Flink 2.x removed the 2-arg createCatalog(String, Map) overload and made the 3-arg
    // (String, Map, org.apache.hadoop.conf.Configuration) overload protected; only the public
    // createCatalog(Context) overload remains usable from outside the iceberg-flink package.
    return new FlinkCatalogFactory()
        .createCatalog(
            new CatalogFactory.Context() {
              /** {@inheritDoc} */
              @Override
              public String getName() {
                return catalogName;
              }

              /** {@inheritDoc} */
              @Override
              public Map<String, String> getOptions() {
                return properties;
              }

              /** {@inheritDoc} */
              @Override
              public ReadableConfig getConfiguration() {
                // FlinkCatalogFactory.createCatalog(Context) does not currently read
                // getConfiguration() (it derives its Hadoop Configuration from
                // FlinkCatalogFactory.clusterHadoopConf() instead), so an empty one is a no-op
                // stub to satisfy the interface. Re-check this on iceberg-flink upgrades.
                return new Configuration();
              }

              /** {@inheritDoc} */
              @Override
              public ClassLoader getClassLoader() {
                return GravitinoIcebergCatalogFlink22.class.getClassLoader();
              }
            });
  }

  /** {@inheritDoc} */
  @Override
  protected CatalogCompat catalogCompat() {
    return CatalogCompatFlink22.INSTANCE;
  }
}
