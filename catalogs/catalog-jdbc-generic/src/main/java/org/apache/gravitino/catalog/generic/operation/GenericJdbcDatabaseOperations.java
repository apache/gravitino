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
package org.apache.gravitino.catalog.generic.operation;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.generic.GenericJdbcMetadataConfig;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;

/** Read-only schema operations for the generic JDBC catalog. */
public class GenericJdbcDatabaseOperations extends JdbcDatabaseOperations {

  private static final String TABLE_SCHEM = "TABLE_SCHEM";

  private GenericJdbcMetadataConfig metadataConfig;

  @Override
  public void initialize(
      DataSource dataSource, JdbcExceptionConverter exceptionMapper, Map<String, String> conf) {
    super.initialize(dataSource, exceptionMapper, conf);
    this.metadataConfig = new GenericJdbcMetadataConfig(conf);
  }

  @Override
  public void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException {
    throw readOnlyException("create schema");
  }

  @Override
  public boolean delete(String databaseName, boolean cascade) {
    throw readOnlyException("drop schema");
  }

  @Override
  public List<String> listDatabases() {
    List<String> schemaNames = new ArrayList<>();
    try (Connection connection = getConnection();
        ResultSet schemas = getSchemas(connection, metadataConfig.schemaPattern())) {
      while (schemas.next()) {
        String schemaName = schemas.getString(TABLE_SCHEM);
        if (schemaName != null && metadataConfig.includesSchema(schemaName)) {
          schemaNames.add(schemaName);
        }
      }
      return schemaNames;
    } catch (SQLException se) {
      throw exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public boolean exist(String databaseName) {
    try (Connection connection = getConnection();
        ResultSet schemas = getSchemas(connection, databaseName)) {
      while (schemas.next()) {
        if (Objects.equals(schemas.getString(TABLE_SCHEM), databaseName)
            && metadataConfig.includesSchema(databaseName)) {
          return true;
        }
      }
      return false;
    } catch (SQLException se) {
      throw exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    if (!exist(databaseName)) {
      throw new NoSuchSchemaException("No such schema: %s", databaseName);
    }

    return JdbcSchema.builder()
        .withName(databaseName)
        .withProperties(ImmutableMap.of())
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  protected boolean supportSchemaComment() {
    return false;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return Set.of();
  }

  ResultSet getSchemas(Connection connection, String schemaPattern) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getSchemas(metadataConfig.catalogPattern(), schemaPattern);
  }

  static UnsupportedOperationException readOnlyException(String operation) {
    return new UnsupportedOperationException(
        "Generic JDBC catalog is read-only and does not support " + operation);
  }
}
