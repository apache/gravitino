/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.starrocks.operations;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.starrocks.utils.StarRocksUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.meta.AuditInfo;

/** Database operations for StarRocks. */
public class StarRocksDatabaseOperations extends JdbcDatabaseOperations {
  public static final String COMMENT_KEY = "comment";

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    throw new NotImplementedException("To be implemented in the future");
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    List<String> allDatabases = listDatabases();
    String dbName =
        allDatabases.stream()
            .filter(db -> db.equals(databaseName))
            .findFirst()
            .orElseThrow(
                () -> new NoSuchSchemaException("Database %s could not be found", databaseName));

    Map<String, String> properties = getDatabaseProperties(databaseName);

    // extract comment from properties
    String comment = properties.remove(COMMENT_KEY);

    return JdbcSchema.builder()
        .withName(dbName)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  private Map<String, String> getDatabaseProperties(String databaseName) {

    String showCreateDatabaseSql = String.format("SHOW CREATE DATABASE `%s`", databaseName);
    StringBuilder createDatabaseSb = new StringBuilder();
    try (final Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(showCreateDatabaseSql);
        ResultSet resultSet = statement.executeQuery()) {
      while (resultSet.next()) {
        createDatabaseSb.append(resultSet.getString("Create Database"));
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
    }

    String createDatabaseSql = createDatabaseSb.toString();

    if (StringUtils.isEmpty(createDatabaseSql)) {
      throw new NoSuchTableException("Database %s does not exist.", databaseName);
    }

    return StarRocksUtils.extractPropertiesFromSql(createDatabaseSql);
  }

  @Override
  protected boolean supportSchemaComment() {
    return true;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of("information_schema");
  }
}
