/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.postgresql.operation;

import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlSchemaOperationsSqlGeneration {

  @Test
  public void testEscapeCommentInGeneratedSql() {
    PostgreSqlSchemaOperations operations = new PostgreSqlSchemaOperations();

    String sql =
        operations.generateCreateDatabaseSql(
            "test_schema", "owner\\'s comment; DROP SCHEMA marker; --", Collections.emptyMap());

    Assertions.assertEquals(
        "CREATE SCHEMA \"test_schema\";COMMENT ON SCHEMA \"test_schema\" "
            + "IS E'owner\\\\''s comment; DROP SCHEMA marker; --'",
        sql);
  }

  @Test
  public void testGenerateCreateDatabaseSqlValidatesSchemaName() {
    PostgreSqlSchemaOperations operations = new PostgreSqlSchemaOperations();

    Assertions.assertEquals(
        "CREATE SCHEMA \"test_schema\";",
        operations.generateCreateDatabaseSql("test_schema", null, Collections.emptyMap()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            operations.generateCreateDatabaseSql(
                "schema\"; DROP TABLE users; --", null, Collections.emptyMap()));
  }

  @Test
  public void testGenerateDropDatabaseSqlValidatesSchemaName() {
    PostgreSqlSchemaOperations operations = new PostgreSqlSchemaOperations();

    Assertions.assertEquals(
        "DROP SCHEMA \"test_schema\" CASCADE",
        operations.generateDropDatabaseSql("test_schema", true));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> operations.generateDropDatabaseSql("schema\"; DROP TABLE users; --", true));
  }
}
