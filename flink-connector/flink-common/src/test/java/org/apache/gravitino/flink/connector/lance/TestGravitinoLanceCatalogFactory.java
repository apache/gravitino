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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestGravitinoLanceCatalogFactory {

  private final GravitinoLanceCatalogFactory factory =
      new GravitinoLanceCatalogFactory() {
        @Override
        protected Catalog newCatalog(
            String catalogName,
            String defaultDatabase,
            SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
            PartitionConverter partitionConverter,
            Map<String, String> catalogOptions) {
          return Mockito.mock(Catalog.class);
        }
      };

  @Test
  void testFactoryMetadata() {
    Assertions.assertEquals("gravitino-lance", factory.factoryIdentifier());
    Assertions.assertEquals("lakehouse-generic", factory.gravitinoCatalogProvider());
    Assertions.assertEquals(
        org.apache.gravitino.Catalog.Type.RELATIONAL, factory.gravitinoCatalogType());
    Assertions.assertSame(LancePropertiesConverter.INSTANCE, factory.catalogPropertiesConverter());
    Assertions.assertSame(
        LancePropertiesConverter.INSTANCE, factory.schemaAndTablePropertiesConverter());
    Assertions.assertSame(DefaultPartitionConverter.INSTANCE, factory.partitionConverter());
    Assertions.assertTrue(factory.requiredOptions().isEmpty());
    Assertions.assertTrue(
        factory.optionalOptions().contains(GravitinoLanceCatalogFactoryOptions.URI));
    Assertions.assertTrue(
        factory.optionalOptions().contains(GravitinoLanceCatalogFactoryOptions.DEFAULT_DATABASE));
    Assertions.assertTrue(
        factory.optionalOptions().contains(GravitinoLanceCatalogFactoryOptions.CATALOG_BACKEND));
    Assertions.assertTrue(
        factory.optionalOptions().contains(GravitinoLanceCatalogFactoryOptions.IMPL));
    Assertions.assertTrue(
        factory.optionalOptions().contains(GravitinoLanceCatalogFactoryOptions.WAREHOUSE));
    Assertions.assertTrue(
        factory.optionalOptions().contains(GravitinoLanceCatalogFactoryOptions.LOCATION));
  }

  @Test
  void testSupportsProvider() {
    Assertions.assertTrue(factory.supportsProvider("lakehouse-generic"));
    Assertions.assertTrue(factory.supportsProvider("LAKEHOUSE-GENERIC"));
    Assertions.assertTrue(factory.supportsProvider("lakehouse-lance"));
    Assertions.assertTrue(factory.supportsProvider("lance"));
    Assertions.assertTrue(factory.supportsProvider("LANCE"));

    Assertions.assertFalse(factory.supportsProvider("lakehouse-iceberg"));
    Assertions.assertFalse(factory.supportsProvider("iceberg"));
    Assertions.assertFalse(factory.supportsProvider("lakehouse-paimon"));
    Assertions.assertFalse(factory.supportsProvider("hive"));
    Assertions.assertFalse(factory.supportsProvider("jdbc"));
  }

  @Test
  void testCreateCatalogDelegatesToNewCatalog() {
    AtomicBoolean called = new AtomicBoolean(false);
    GravitinoLanceCatalogFactory testFactory =
        new GravitinoLanceCatalogFactory() {
          @Override
          protected Catalog newCatalog(
              String catalogName,
              String defaultDatabase,
              SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
              PartitionConverter partitionConverter,
              Map<String, String> catalogOptions) {
            called.set(true);
            Assertions.assertEquals("my_lance_catalog", catalogName);
            Assertions.assertEquals("custom_db", defaultDatabase);
            Assertions.assertEquals("http://localhost:8090/lance", catalogOptions.get("uri"));
            return Mockito.mock(Catalog.class);
          }
        };

    Map<String, String> options =
        ImmutableMap.of(
            CommonCatalogOptions.CATALOG_TYPE.key(), "gravitino-lance",
            CommonCatalogOptions.DEFAULT_DATABASE_KEY.key(), "custom_db",
            "uri", "http://localhost:8090/lance");

    CatalogFactory.Context context =
        new CatalogFactory.Context() {
          @Override
          public String getName() {
            return "my_lance_catalog";
          }

          @Override
          public Map<String, String> getOptions() {
            return options;
          }

          @Override
          public org.apache.flink.configuration.ReadableConfig getConfiguration() {
            return Configuration.fromMap(options);
          }

          @Override
          public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
          }
        };

    Catalog catalog = testFactory.createCatalog(context);
    Assertions.assertNotNull(catalog);
    Assertions.assertTrue(called.get());
  }
}
