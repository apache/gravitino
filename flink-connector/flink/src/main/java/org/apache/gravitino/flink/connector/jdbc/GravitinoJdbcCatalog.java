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
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;

/**
 * The GravitinoJdbcCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the JdbcCatalog class.
 */
public class GravitinoJdbcCatalog extends BaseCatalog {

  private final JdbcCatalog jdbcCatalog;

  private final CatalogFactory.Context context;

  protected GravitinoJdbcCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      PropertiesConverter propertiesConverter,
      PartitionConverter partitionConverter) {
    super(context.getName(), defaultDatabase, propertiesConverter, partitionConverter);
    this.context = context;
    JdbcCatalogFactory jdbcCatalogFactory = new JdbcCatalogFactory();
    this.jdbcCatalog = (JdbcCatalog) jdbcCatalogFactory.createCatalog(context);
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return jdbcCatalog;
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new JdbcDynamicTableFactory());
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    CatalogBaseTable table = super.getTable(tablePath);
    Map<String, String> contextOptions = context.getOptions();
    Map<String, String> tableOptions = table.getOptions();
    tableOptions.remove("engine");
    tableOptions.put(
        "url",
        contextOptions.get(JdbcPropertiesConstants.FLINK_JDBC_URL)
            + "/"
            + tablePath.getDatabaseName());
    tableOptions.put("table-name", tablePath.getObjectName());
    tableOptions.put("username", contextOptions.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    tableOptions.put("password", contextOptions.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
    return table;
  }
}
