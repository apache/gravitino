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
package org.apache.gravitino.catalog.starrocks.operation;

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestStarRocksDatabaseOperations extends TestStarRocks {

  @Test
  public void testBaseOperationDatabase() {
    String databaseName = RandomNameUtils.genRandomName("it_db");
    String comment = "";
    // StarRocks can't get properties after set, so the test case can't include properties
    testBaseOperation(databaseName, Collections.emptyMap(), comment);

    // recreate database, get exception.
    Assertions.assertThrowsExactly(
        SchemaAlreadyExistsException.class,
        () -> DATABASE_OPERATIONS.create(databaseName, "", Collections.emptyMap()));

    testDropDatabase(databaseName);
  }

  @Test
  void testListSystemDatabase() {
    List<String> databaseNames = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databaseNames.contains("information_schema"));
  }
}
