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
package org.apache.gravitino.catalog.oceanbase.operation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/** Database operations for OceanBase. */
public class OceanBaseDatabaseOperations extends JdbcDatabaseOperations {

  public static final Set<String> SYS_OCEANBASE_DATABASE_NAMES = createSysOceanBaseDatabaseNames();

  private static Set<String> createSysOceanBaseDatabaseNames() {
    Set<String> set = new HashSet<>();
    set.add("information_schema");
    set.add("mysql");
    set.add("sys");
    set.add("oceanbase");
    return Collections.unmodifiableSet(set);
  }

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {

    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  protected boolean isSystemDatabase(String dbName) {
    return SYS_OCEANBASE_DATABASE_NAMES.contains(dbName.toLowerCase(Locale.ROOT));
  }
}
