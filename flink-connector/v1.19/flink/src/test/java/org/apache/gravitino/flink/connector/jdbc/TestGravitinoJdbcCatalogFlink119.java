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

import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.mockito.Mockito;

/** JDBC catalog tests for Flink 1.19. */
public class TestGravitinoJdbcCatalogFlink119 extends TestGravitinoJdbcCatalog {

  @Override
  protected GravitinoJdbcCatalog createNativeCatalog(
      CatalogFactory.Context context, String defaultDatabase) {
    return new GravitinoJdbcCatalogFlink119(
        context,
        defaultDatabase,
        Mockito.mock(SchemaAndTablePropertiesConverter.class),
        Mockito.mock(PartitionConverter.class)) {
      @Override
      protected Catalog catalog() {
        throw new NoSuchCatalogException("Catalog %s does not exist", "test");
      }
    };
  }
}
