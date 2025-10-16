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

import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcUtil.SchemaVersion;

// Use Iceberg package to reuse JdbcUtil related classes.
public class JdbcCatalogWithMetadataLocation extends JdbcCatalog
    implements SupportsMetadataLocation {
  private String catalogName;
  private JdbcClientPool connections;
  private SchemaVersion schemaVersion;

  public JdbcCatalogWithMetadataLocation(boolean initializeCatalogTables) {
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
      table = JdbcUtil.loadTable(schemaVersion, connections, catalogName, tableIdentifier);
    } catch (Exception e) {
      return null;
    }

    return table.get(METADATA_LOCATION_PROP);
  }

  private void loadFields() {
    try {
      Class<?> baseClass = JdbcCatalog.class;
      this.catalogName = (String) FieldUtils.readField(baseClass, "catalogName", true);
      this.connections = (JdbcClientPool) FieldUtils.readField(baseClass, "connections", true);
      this.schemaVersion =
          (JdbcUtil.SchemaVersion) FieldUtils.readField(baseClass, "schemaVersion", true);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
