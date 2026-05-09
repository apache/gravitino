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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcCatalogPropertiesMetadata {

  @Test
  public void testPoolSizePropertiesAreNotHidden() {
    JdbcCatalogPropertiesMetadata propertiesMetadata = new JdbcCatalogPropertiesMetadata();

    Assertions.assertFalse(propertiesMetadata.isHiddenProperty(JdbcConfig.POOL_MIN_SIZE.getKey()));
    Assertions.assertFalse(propertiesMetadata.isHiddenProperty(JdbcConfig.POOL_MAX_SIZE.getKey()));
  }

  @Test
  public void testCatalogPropertiesKeepPoolSizeAfterLoad() {
    Map<String, String> createProperties =
        ImmutableMap.<String, String>builder()
            .put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:")
            .put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC")
            .put(JdbcConfig.USERNAME.getKey(), "user")
            .put(JdbcConfig.PASSWORD.getKey(), "password")
            .put(JdbcConfig.POOL_MIN_SIZE.getKey(), "5")
            .put(JdbcConfig.POOL_MAX_SIZE.getKey(), "20")
            .build();

    CatalogEntity catalogEntity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("jdbc_catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("jdbc-test")
            .withComment("test")
            .withProperties(createProperties)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JdbcCatalog loadCatalog =
        new TestJdbcCatalog()
            .withCatalogConf(catalogEntity.getProperties())
            .withCatalogEntity(catalogEntity);

    Map<String, String> loadedProperties = loadCatalog.properties();
    Assertions.assertEquals("5", loadedProperties.get(JdbcConfig.POOL_MIN_SIZE.getKey()));
    Assertions.assertEquals("20", loadedProperties.get(JdbcConfig.POOL_MAX_SIZE.getKey()));
  }

  private static class TestJdbcCatalog extends JdbcCatalog {
    @Override
    public String shortName() {
      return "jdbc-test";
    }

    @Override
    protected JdbcTypeConverter createJdbcTypeConverter() {
      return new SqliteTypeConverter();
    }

    @Override
    protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
      return new SqliteDatabaseOperations(":memory:");
    }

    @Override
    protected JdbcTableOperations createJdbcTableOperations() {
      return new SqliteTableOperations();
    }

    @Override
    protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
      return new SqliteColumnDefaultValueConverter();
    }
  }
}
