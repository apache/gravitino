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

package org.apache.gravitino.catalog.jdbc.operation;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for managing databases in a JDBC data store. */
public abstract class JdbcDatabaseOperations implements DatabaseOperation {

  public static final Logger LOG = LoggerFactory.getLogger(JdbcDatabaseOperations.class);

  protected DataSource dataSource;
  protected JdbcExceptionConverter exceptionMapper;

  @Override
  public void initialize(
      DataSource dataSource, JdbcExceptionConverter exceptionMapper, Map<String, String> conf) {
    this.dataSource = dataSource;
    this.exceptionMapper = exceptionMapper;
  }

  @Override
  public void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException {
    LOG.info("Beginning to create database {}", databaseName);
    try (final Connection connection = getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          connection, generateCreateDatabaseSql(databaseName, comment, properties));
      LOG.info("Finished creating database {}", databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public boolean delete(String databaseName, boolean cascade) {
    LOG.info("Beginning to drop database {}", databaseName);
    try {
      dropDatabase(databaseName, cascade);
      LOG.info("Finished dropping database {}", databaseName);
    } catch (NoSuchSchemaException e) {
      return false;
    }
    return true;
  }

  @Override
  public List<String> listDatabases() {
    List<String> databaseNames = new ArrayList<>();
    try (final Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getCatalogs();
      while (resultSet.next()) {
        String databaseName = resultSet.getString("TABLE_CAT");
        if (!isSystemDatabase(databaseName)) {
          databaseNames.add(databaseName);
        }
      }
      return databaseNames;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected void dropDatabase(String databaseName, boolean cascade) {
    try (final Connection connection = getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, generateDropDatabaseSql(databaseName, cascade));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * @param databaseName The name of the database.
   * @param comment The comment of the database.
   * @param properties The properties of the database.
   * @return the SQL statement to create a database with the given name and comment.
   */
  protected abstract String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties);

  /**
   * @param databaseName The name of the database.
   * @param cascade cascade If set to true, drops all the tables in the schema as well.
   * @return the SQL statement to drop a database with the given name.
   */
  protected abstract String generateDropDatabaseSql(String databaseName, boolean cascade);

  protected Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * Check whether it is a system database.
   *
   * @param dbName The name of the database.
   * @return false for all cases.
   */
  protected boolean isSystemDatabase(String dbName) {
    return false;
  }
}
