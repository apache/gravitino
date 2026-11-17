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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.ClosableJdbcCatalog;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.utils.IcebergTableLocationUtils;
import org.apache.iceberg.MetastoreRegisterTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.jdbc.JdbcUtil.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Use Iceberg package to reuse JdbcUtil related classes.
public class JdbcCatalogWithMetadataLocationSupport extends ClosableJdbcCatalog
    implements SupportsMetadataLocation {
  private static final Logger LOG =
      LoggerFactory.getLogger(JdbcCatalogWithMetadataLocationSupport.class);

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

  /** {@inheritDoc} */
  @Override
  public Optional<Map<TableIdentifier, String>> metadataLocationCandidates(String location) {
    List<String> patterns = IcebergTableLocationUtils.metadataLocationPatterns(location);
    String predicate =
        String.join(
            " OR ",
            Collections.nCopies(patterns.size(), METADATA_LOCATION_PROP + " LIKE ? ESCAPE '!'"));
    String query =
        "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables"
            + " WHERE catalog_name = ?"
            + (jdbcSchemaVersion == SchemaVersion.V0
                ? ""
                : " AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)")
            + " AND ("
            + predicate
            + ")";
    try {
      return Optional.of(
          jdbcConnections.run(
              connection -> {
                Map<TableIdentifier, String> rows = new LinkedHashMap<>();
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                  statement.setString(1, jdbcCatalogName);
                  for (int i = 0; i < patterns.size(); i++) {
                    statement.setString(i + 2, patterns.get(i));
                  }
                  try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                      rows.put(
                          TableIdentifier.of(
                              JdbcUtil.stringToNamespace(result.getString(1)), result.getString(2)),
                          result.getString(3));
                    }
                  }
                }
                return rows;
              }));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during metadata pointer lookup");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to look up metadata pointers");
    }
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

  /**
   * Registers an already read metadata snapshot without opening the input file again.
   *
   * @param identifier destination table
   * @param metadata metadata snapshot read and authorized for this request
   * @param overwrite whether an existing registration can be replaced
   * @return registered table
   */
  public Table registerTable(
      TableIdentifier identifier, TableMetadata metadata, boolean overwrite) {
    return MetastoreRegisterTableUtils.registerTable(
        this, identifier, metadata, overwrite, this::overwriteMetadataLocation);
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
