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

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.flink.connector.utils.CatalogCompatFlink118;

public class GravitinoJdbcCatalogFlink118 extends GravitinoJdbcCatalog {

  public GravitinoJdbcCatalogFlink118(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    super(context, defaultDatabase, schemaAndTablePropertiesConverter, partitionConverter);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected AbstractCatalog createInnerCatalog(CatalogFactory.Context context) {
    // JdbcCatalogFactory is deprecated in Flink 1.18's jdbc connector but has no non-deprecated
    // replacement yet; referenced by FQN so the deprecation warning stays suppressible here
    // instead of leaking to an unsuppressible import-level warning.
    return (AbstractCatalog)
        new org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory()
            .createCatalog(context);
  }

  @Override
  protected CatalogCompat catalogCompat() {
    return CatalogCompatFlink118.INSTANCE;
  }
}
