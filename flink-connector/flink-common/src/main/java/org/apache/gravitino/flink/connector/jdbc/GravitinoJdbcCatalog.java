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

import java.util.Optional;
import java.util.Properties;
import org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;

/**
 * The GravitinoJdbcCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the JdbcCatalog class.
 */
public class GravitinoJdbcCatalog extends BaseCatalog {

  private final AbstractCatalog jdbcCatalog;

  public enum JdbcCatalogType {
    MYSQL("org.apache.flink.connector.jdbc.mysql.database.catalog.MySqlCatalog"),
    POSTGRESQL("org.apache.flink.connector.jdbc.postgres.database.catalog.PostgresCatalog");

    private final String className;

    JdbcCatalogType(String className) {
      this.className = className;
    }

    String className() {
      return className;
    }
  }

  protected GravitinoJdbcCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      JdbcCatalogType catalogType,
      String driver) {
    super(
        context.getName(),
        context.getOptions(),
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    JdbcCatalogFactory jdbcCatalogFactory = new JdbcCatalogFactory();
    AbstractCatalog createdCatalog;
    try {
      createdCatalog = (AbstractCatalog) jdbcCatalogFactory.createCatalog(context);
    } catch (ClassCastException exception) {
      createdCatalog =
          createCatalogWithNewJdbcPackages(context, defaultDatabase, catalogType, driver);
    }
    this.jdbcCatalog = createdCatalog;
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return jdbcCatalog;
  }

  @Override
  public Optional<Factory> getFactory() {
    try {
      return Optional.of(new JdbcDynamicTableFactory());
    } catch (NoClassDefFoundError error) {
      return Optional.of(loadJdbcDynamicTableFactory());
    }
  }

  private Factory loadJdbcDynamicTableFactory() {
    try {
      return (Factory)
          Class.forName("org.apache.flink.connector.jdbc.core.table.JdbcDynamicTableFactory")
              .getConstructor()
              .newInstance();
    } catch (ReflectiveOperationException exception) {
      throw new IllegalStateException("Failed to load JDBC dynamic table factory.", exception);
    }
  }

  private AbstractCatalog createCatalogWithNewJdbcPackages(
      CatalogFactory.Context context,
      String defaultDatabase,
      JdbcCatalogType catalogType,
      String driver) {
    String baseUrl = context.getOptions().get(JdbcPropertiesConstants.FLINK_JDBC_URL);
    String username = context.getOptions().get(JdbcPropertiesConstants.FLINK_JDBC_USER);
    String password = context.getOptions().get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD);
    Properties properties = new Properties();
    if (username != null && password != null) {
      properties.setProperty("user", username);
      properties.setProperty("password", password);
    }
    if (driver != null) {
      properties.setProperty("driver", driver);
    }
    try {
      Class<?> catalogClass = Class.forName(catalogType.className());
      return (AbstractCatalog)
          catalogClass
              .getConstructor(
                  ClassLoader.class, String.class, String.class, String.class, Properties.class)
              .newInstance(
                  context.getClassLoader(),
                  context.getName(),
                  defaultDatabase,
                  baseUrl,
                  properties);
    } catch (ReflectiveOperationException exception) {
      throw new IllegalStateException(
          "Failed to create JDBC catalog for Flink 1.20+ packages.", exception);
    }
  }
}
