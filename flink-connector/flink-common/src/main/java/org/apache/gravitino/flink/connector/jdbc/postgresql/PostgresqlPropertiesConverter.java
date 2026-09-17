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

package org.apache.gravitino.flink.connector.jdbc.postgresql;

import com.google.common.base.Strings;
import java.util.Map;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.flink.connector.jdbc.GravitinoJdbcCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.flink.connector.jdbc.JdbcPropertiesConverter;

public class PostgresqlPropertiesConverter extends JdbcPropertiesConverter {

  private PostgresqlPropertiesConverter() {}

  public static final PostgresqlPropertiesConverter INSTANCE = new PostgresqlPropertiesConverter();

  @Override
  protected String defaultDriverName() {
    return "org.postgresql.Driver";
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoJdbcCatalogFactoryOptions.POSTGRESQL_IDENTIFIER;
  }

  @Override
  protected String getConnectionDatabase(
      Map<String, String> flinkCatalogProperties, ObjectPath tablePath) {
    // For PostgreSQL, the Flink "database" is a Gravitino schema, not the PostgreSQL database
    // that the JDBC connection must target. The connection must use the catalog's configured
    // jdbc-database instead, which JdbcPropertiesConverter#toFlinkCatalogProperties defaults into
    // the 'default-database' option.
    String database =
        flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(database),
        JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE + " should not be null or empty.");
    return database;
  }

  @Override
  protected String getTableName(ObjectPath tablePath) {
    return tablePath.getDatabaseName() + "." + tablePath.getObjectName();
  }
}
