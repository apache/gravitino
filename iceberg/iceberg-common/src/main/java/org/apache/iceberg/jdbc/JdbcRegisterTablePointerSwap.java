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

import java.sql.SQLException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.jdbc.JdbcUtil.SchemaVersion;

/** JDBC metastore pointer swap used by {@link org.apache.iceberg.RegisterTableOverwrite}. */
public final class JdbcRegisterTablePointerSwap {

  private JdbcRegisterTablePointerSwap() {}

  public static void swap(
      JdbcCatalog catalog,
      TableIdentifier tableIdentifier,
      String oldMetadataLocation,
      String newMetadataLocation) {
    try {
      String catalogName = (String) FieldUtils.readField(catalog, "catalogName", true);
      JdbcClientPool connections =
          (JdbcClientPool) FieldUtils.readField(catalog, "connections", true);
      SchemaVersion schemaVersion =
          (SchemaVersion) FieldUtils.readField(catalog, "schemaVersion", true);

      int updatedRecords =
          JdbcUtil.updateTable(
              schemaVersion,
              connections,
              catalogName,
              tableIdentifier,
              newMetadataLocation,
              oldMetadataLocation);

      if (updatedRecords != 1) {
        throw new CommitFailedException(
            "Failed to update table %s from catalog %s", tableIdentifier, catalogName);
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to read JDBC catalog fields", e);
    } catch (SQLException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new UncheckedSQLException(e, "Failed to update table %s", tableIdentifier);
    }
  }
}
