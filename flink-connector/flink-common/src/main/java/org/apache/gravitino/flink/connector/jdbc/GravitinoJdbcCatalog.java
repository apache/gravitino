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

import java.util.Map;
import java.util.Optional;
import org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;

/**
 * The GravitinoJdbcCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the JdbcCatalog class.
 */
public class GravitinoJdbcCatalog extends BaseCatalog {

  private final CatalogFactory.Context context;
  private AbstractCatalog jdbcCatalog;

  protected GravitinoJdbcCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    super(
        context.getName(),
        context.getOptions(),
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.context = context;
    this.jdbcCatalog = null;
  }

  protected GravitinoJdbcCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      AbstractCatalog jdbcCatalog) {
    super(
        context.getName(),
        context.getOptions(),
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.context = context;
    this.jdbcCatalog = jdbcCatalog;
  }

  @Override
  public void open() throws CatalogException {
    if (jdbcCatalog == null) {
      try {
        applyJdbcCredential(catalog(), context.getOptions());
      } catch (NoSuchCatalogException ignored) {
        // During CREATE CATALOG, open() is called before the catalog is stored in Gravitino.
        // In this case credentials are already present in the user-provided options.
      }
      this.jdbcCatalog = (AbstractCatalog) new JdbcCatalogFactory().createCatalog(context);
    }
    super.open();
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return jdbcCatalog;
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new JdbcDynamicTableFactory());
  }

  /**
   * Overwrites the Flink JDBC user and password in {@code options} with credentials obtained from
   * the server via credential vending, if available. Falls back to the existing options if the
   * catalog does not support credential vending or no JDBC credential is returned.
   *
   * @param catalog the Gravitino catalog client
   * @param options the mutable Flink catalog options map to update
   */
  static void applyJdbcCredential(Catalog catalog, Map<String, String> options) {
    if (!(catalog instanceof SupportsCredentials)) {
      return;
    }
    for (Credential credential : ((SupportsCredentials) catalog).getCredentials()) {
      if (credential instanceof JdbcCredential) {
        JdbcCredential jdbcCredential = (JdbcCredential) credential;
        options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, jdbcCredential.jdbcUser());
        options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, jdbcCredential.jdbcPassword());
        return;
      }
    }
  }
}
