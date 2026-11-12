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
package org.apache.gravitino.catalog.postgresql.operation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Regression tests for SQL quoting of existing native identifiers. */
public class TestPostgreSqlTableOperationsNativeIdentifiers extends PostgreSqlTableOperations {
  @Test
  void testDropAndRenameQuoteExistingIdentifiers() {
    String name = "tab\"le\\path; --";
    String quoted = "\"tab\"\"le\\path; --\"";
    Assertions.assertEquals("DROP TABLE " + quoted, generateDropTableSql(name));
    Assertions.assertEquals(
        "ALTER TABLE " + quoted + " RENAME TO \"new_table\"",
        generateRenameTableSql(name, "new_table"));
  }
}
