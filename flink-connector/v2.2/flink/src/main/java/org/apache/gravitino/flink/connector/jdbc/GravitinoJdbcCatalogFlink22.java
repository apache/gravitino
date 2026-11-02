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

package org.apache.gravitino.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.flink.connector.utils.CatalogCompatFlink22;

/**
 * {@link GravitinoJdbcCatalog} implementation for Flink 2.2. Uses the {@code
 * org.apache.flink.connector.jdbc.core.*} factory packages shipped with {@code
 * flink-connector-jdbc-core}/{@code -mysql}/{@code -postgres} 4.1.0-2.2.
 */
public class GravitinoJdbcCatalogFlink22 extends GravitinoJdbcCatalog {

  /**
   * Creates a catalog backed by the Flink 2.2 connector.
   *
   * @param context the catalog factory context
   * @param defaultDatabase the default database name
   * @param schemaAndTablePropertiesConverter the converter for schemas and table properties
   * @param partitionConverter the converter for partition specifications
   */
  public GravitinoJdbcCatalogFlink22(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    super(context, defaultDatabase, schemaAndTablePropertiesConverter, partitionConverter);
  }

  /** {@inheritDoc} */
  @Override
  protected AbstractCatalog createInnerCatalog(CatalogFactory.Context context) {
    return (AbstractCatalog) new JdbcCatalogFactory().createCatalog(context);
  }

  /** {@inheritDoc} */
  @Override
  protected CatalogCompat catalogCompat() {
    return CatalogCompatFlink22.INSTANCE;
  }
}
