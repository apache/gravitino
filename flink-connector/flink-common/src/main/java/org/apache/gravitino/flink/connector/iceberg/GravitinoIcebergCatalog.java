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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gravitino Iceberg Catalog. */
public abstract class GravitinoIcebergCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoIcebergCatalog.class);

  private final String icebergCatalogName;
  // Mutable copy so credential injection in open() propagates to the inner Iceberg catalog.
  private final Map<String, String> mutableIcebergProperties;
  private AbstractCatalog icebergCatalog;

  protected GravitinoIcebergCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      Map<String, String> icebergCatalogProperties) {
    super(
        catalogName,
        catalogOptions,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.icebergCatalogName = catalogName;
    this.mutableIcebergProperties = new HashMap<>(icebergCatalogProperties);
  }

  protected GravitinoIcebergCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      AbstractCatalog icebergCatalog) {
    super(
        catalogName,
        catalogOptions,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.icebergCatalogName = catalogName;
    this.mutableIcebergProperties = null;
    this.icebergCatalog = icebergCatalog;
  }

  @Override
  public void open() throws CatalogException {
    if (icebergCatalog == null) {
      try {
        CredentialPropertyUtils.applyIcebergCredentials(
            CredentialPropertyUtils.getCredentials(catalog()), mutableIcebergProperties);
      } catch (NoSuchCatalogException e) {
        LOG.warn(
            "Catalog '{}' not found in Gravitino during open(); credential injection skipped."
                + " This is expected during CREATE CATALOG.",
            catalogName(),
            e);
      }
      this.icebergCatalog =
          asAbstractCatalog(
              createInnerIcebergCatalog(icebergCatalogName, mutableIcebergProperties));
    }
    super.open();
  }

  /**
   * Creates the inner Iceberg {@code FlinkCatalogFactory}-backed catalog. Subclasses for different
   * Flink versions implement this because {@code FlinkCatalogFactory.createCatalog}'s signature has
   * changed across Flink major versions (e.g. Flink 2.x removed the public 2-arg {@code
   * createCatalog(String, Map)} overload used by Flink 1.x and made the 3-arg {@code
   * createCatalog(String, Map, Configuration)} overload protected, leaving only the public {@code
   * createCatalog(Context)} overload usable from outside the iceberg-flink package).
   *
   * @param catalogName the Iceberg catalog name
   * @param properties the Iceberg catalog properties
   * @return the created inner catalog
   */
  protected abstract Object createInnerIcebergCatalog(
      String catalogName, Map<String, String> properties);

  @Override
  public Optional<Factory> getFactory() {
    Preconditions.checkState(icebergCatalog != null, "Catalog '%s' has not been opened", getName());
    return icebergCatalog.getFactory();
  }

  @Override
  protected AbstractCatalog realCatalog() {
    Preconditions.checkState(icebergCatalog != null, "Catalog '%s' has not been opened", getName());
    return icebergCatalog;
  }

  protected static AbstractCatalog asAbstractCatalog(Object catalog) {
    Preconditions.checkState(
        catalog instanceof AbstractCatalog,
        "Expected AbstractCatalog from FlinkCatalogFactory but got %s.",
        catalog.getClass().getName());
    return (AbstractCatalog) catalog;
  }
}
