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
package org.apache.gravitino.catalog.hologres.operation;

import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/**
 * Table operations for Hologres.
 *
 * <p>Hologres is PostgreSQL-compatible, so most table operations follow PostgreSQL conventions.
 * However, Hologres has specific features like table properties (orientation, distribution_key,
 * etc.) that are handled through the WITH clause in CREATE TABLE statements.
 *
 * <p>TODO: Full implementation will be added in a follow-up PR.
 */
public class HologresTableOperations extends JdbcTableOperations
    implements RequireDatabaseOperation {

  public static final String HOLO_QUOTE = "\"";

  @Override
  public void setDatabaseOperation(DatabaseOperation databaseOperation) {
    // Will be implemented in a follow-up PR.
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    throw new UnsupportedOperationException(
        "Hologres table creation will be implemented in a follow-up PR.");
  }

  @Override
  protected String generateAlterTableSql(
      String schemaName, String tableName, TableChange... changes) {
    throw new UnsupportedOperationException(
        "Hologres table alteration will be implemented in a follow-up PR.");
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "Hologres does not support purge table in Gravitino, please use drop table");
  }
}
