/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.jdbc;

import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.ClosableJdbcCatalog;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.iceberg.MetastoreRegisterTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.jdbc.JdbcUtil.SchemaVersion;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Use Iceberg package to reuse JdbcUtil related classes.
public class JdbcCatalogWithMetadataLocationSupport extends ClosableJdbcCatalog
    implements SupportsMetadataLocation {
  private static final Logger LOG =
      LoggerFactory.getLogger(JdbcCatalogWithMetadataLocationSupport.class);

  /**
   * Name of the supporting index created on {@value JdbcUtil#CATALOG_TABLE_VIEW_NAME} for
   * PostgreSQL backends. See {@link #maybeCreateNamespaceIndex(Map)} for why it's needed.
   */
  private static final String NAMESPACE_INDEX_NAME = "gravitino_iceberg_tables_namespace_pattern";

  private static final String POSTGRESQL_PRODUCT_NAME = "PostgreSQL";

  private String jdbcCatalogName;
  private JdbcClientPool jdbcConnections;
  private SchemaVersion jdbcSchemaVersion;

  public JdbcCatalogWithMetadataLocationSupport(boolean initializeCatalogTables) {
    super(null, null, initializeCatalogTables);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    loadFields();
    maybeCreateNamespaceIndex(properties);
  }

  @Override
  public String metadataLocation(TableIdentifier tableIdentifier) {
    Map<String, String> table;

    try {
      table =
          JdbcUtil.loadTable(jdbcSchemaVersion, jdbcConnections, jdbcCatalogName, tableIdentifier);
    } catch (Exception e) {
      return null;
    }

    return table.get(METADATA_LOCATION_PROP);
  }

  /**
   * Check if the JDBC catalog schema version supports view operations. View operations are
   * supported from V1 schema version onwards.
   *
   * @return true if the schema version supports view operations, false otherwise
   */
  public boolean supportsViewsWithSchemaVersion() {
    // V0 doesn't support views, only V1 and later versions do
    return jdbcSchemaVersion != null && jdbcSchemaVersion != SchemaVersion.V0;
  }

  /**
   * Registers a table from an existing metadata file, optionally overwriting an existing
   * registration.
   *
   * @param identifier table identifier to register
   * @param metadataFileLocation location of the metadata file to register
   * @param overwrite whether to overwrite an existing table registration
   * @return the registered table
   */
  @Override
  public Table registerTable(
      TableIdentifier identifier, String metadataFileLocation, boolean overwrite) {
    return MetastoreRegisterTableUtils.registerTable(
        this, identifier, metadataFileLocation, overwrite, this::overwriteMetadataLocation);
  }

  private void overwriteMetadataLocation(
      TableIdentifier tableIdentifier, String oldMetadataLocation, String newMetadataLocation) {
    try {
      int updatedRecords =
          JdbcUtil.updateTable(
              jdbcSchemaVersion,
              jdbcConnections,
              jdbcCatalogName,
              tableIdentifier,
              newMetadataLocation,
              oldMetadataLocation);

      if (updatedRecords == 1) {
        LOG.debug("Successfully committed to existing table: {}", tableIdentifier);
      } else {
        throw new CommitFailedException(
            "Failed to update table %s from catalog %s", tableIdentifier, jdbcCatalogName);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during commit");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Unknown failure");
    }
  }

  /**
   * Creates a supporting index for the hierarchical-namespace existence and listing queries
   * Iceberg's {@code JdbcCatalog} issues against {@value JdbcUtil#CATALOG_TABLE_VIEW_NAME} - an
   * {@code OR(exact match, LIKE prefix-match)} predicate over {@value JdbcUtil#CATALOG_NAME} and
   * {@value JdbcUtil#TABLE_NAMESPACE} that the table's primary key alone can only serve as a
   * sequential scan, since a plain b-tree index built with PostgreSQL's default locale-aware
   * operator class cannot bound a {@code LIKE} range scan. At a large number of tables this makes
   * every namespace/table existence check, and therefore every namespace create/drop, scan the
   * whole table.
   *
   * <p>PostgreSQL-only: detected at runtime via {@link java.sql.DatabaseMetaData}, so this is a
   * no-op for MySQL, SQLite, H2, and any other JDBC backend. Controlled by {@link
   * IcebergConstants#ICEBERG_JDBC_CREATE_NAMESPACE_INDEX} (default enabled). Never fails catalog
   * initialization: a missing index is a performance issue, not a correctness one, so an operator
   * whose database role lacks DDL privileges should still be able to start the catalog - a warning
   * is logged instead.
   *
   * @param properties the properties passed to {@link #initialize(String, Map)}
   */
  private void maybeCreateNamespaceIndex(Map<String, String> properties) {
    if (!PropertyUtil.propertyAsBoolean(
        properties, IcebergConstants.ICEBERG_JDBC_CREATE_NAMESPACE_INDEX, true)) {
      return;
    }

    try {
      jdbcConnections.run(
          conn -> {
            if (!POSTGRESQL_PRODUCT_NAME.equals(conn.getMetaData().getDatabaseProductName())) {
              return null;
            }
            String sql =
                "CREATE INDEX IF NOT EXISTS "
                    + NAMESPACE_INDEX_NAME
                    + " ON "
                    + JdbcUtil.CATALOG_TABLE_VIEW_NAME
                    + " ("
                    + JdbcUtil.CATALOG_NAME
                    + ", "
                    + JdbcUtil.TABLE_NAMESPACE
                    + " text_pattern_ops)";
            try (Statement stmt = conn.createStatement()) {
              stmt.execute(sql);
            }
            return null;
          });
    } catch (Exception e) {
      LOG.warn(
          "Failed to create supporting index {} on {}; namespace/table existence checks may "
              + "become slow as the catalog grows. This does not affect correctness and catalog "
              + "initialization is continuing. Set {}=false to silence this warning.",
          NAMESPACE_INDEX_NAME,
          JdbcUtil.CATALOG_TABLE_VIEW_NAME,
          IcebergConstants.ICEBERG_JDBC_CREATE_NAMESPACE_INDEX,
          e);
    }
  }

  private void loadFields() {
    try {
      this.jdbcCatalogName = (String) FieldUtils.readField(this, "catalogName", true);
      Preconditions.checkState(
          jdbcCatalogName != null, "Failed to get catalogName field from JDBC catalog");
      this.jdbcConnections = (JdbcClientPool) FieldUtils.readField(this, "connections", true);
      Preconditions.checkState(
          jdbcConnections != null, "Failed to get connections field from JDBC catalog");
      this.jdbcSchemaVersion =
          (JdbcUtil.SchemaVersion) FieldUtils.readField(this, "schemaVersion", true);
      Preconditions.checkState(
          jdbcSchemaVersion != null, "Failed to get schemaVersion field from JDBC catalog");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
