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
package org.apache.gravitino.catalog.generic;

import org.apache.gravitino.catalog.generic.converter.GenericJdbcTypeConverter;
import org.apache.gravitino.catalog.generic.operation.GenericJdbcDatabaseOperations;
import org.apache.gravitino.catalog.generic.operation.GenericJdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;

/** Generic read-only JDBC catalog based on standard {@link java.sql.DatabaseMetaData}. */
public class GenericJdbcCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-generic";
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new GenericJdbcTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new GenericJdbcDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new GenericJdbcTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new JdbcColumnDefaultValueConverter();
  }
}
