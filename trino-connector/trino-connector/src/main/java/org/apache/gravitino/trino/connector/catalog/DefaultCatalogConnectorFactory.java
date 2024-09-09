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
package org.apache.gravitino.trino.connector.catalog;

import io.trino.spi.TrinoException;
import java.util.HashMap;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.hive.HiveConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.iceberg.IcebergConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.jdbc.postgresql.PostgreSQLConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.memory.MemoryConnectorAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class use to create CatalogConnectorContext instance by given catalog. */
public class DefaultCatalogConnectorFactory implements CatalogConnectorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCatalogConnectorFactory.class);

  protected final HashMap<String, CatalogConnectorContext.Builder> catalogBuilders =
      new HashMap<>();

  public DefaultCatalogConnectorFactory(GravitinoConfig config) {
    catalogBuilders.put("hive", new CatalogConnectorContext.Builder(new HiveConnectorAdapter()));
    catalogBuilders.put(
        "memory", new CatalogConnectorContext.Builder(new MemoryConnectorAdapter()));
    catalogBuilders.put(
        "lakehouse-iceberg", new CatalogConnectorContext.Builder(new IcebergConnectorAdapter()));
    catalogBuilders.put(
        "jdbc-mysql", new CatalogConnectorContext.Builder(new MySQLConnectorAdapter()));
    catalogBuilders.put(
        "jdbc-postgresql", new CatalogConnectorContext.Builder(new PostgreSQLConnectorAdapter()));
    LOG.info("Start the DefaultCatalogConnectorFactory");
  }

  public CatalogConnectorContext.Builder createCatalogConnectorContextBuilder(
      GravitinoCatalog catalog) {
    String catalogProvider = catalog.getProvider();
    CatalogConnectorContext.Builder builder = catalogBuilders.get(catalogProvider);
    if (builder == null) {
      String message = String.format("Unsupported catalog provider %s.", catalogProvider);
      LOG.error(message);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_UNSUPPORTED_CATALOG_PROVIDER, message);
    }

    // Avoid using the same builder object to prevent catalog creation errors.
    return builder.clone(catalog);
  }
}
