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
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcUtil.SchemaVersion;

// Use Iceberg package to reuse JdbcUtil related classes.
public class JdbcCatalogWithMetadataLocationSupport extends JdbcCatalog
    implements SupportsMetadataLocation {
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
