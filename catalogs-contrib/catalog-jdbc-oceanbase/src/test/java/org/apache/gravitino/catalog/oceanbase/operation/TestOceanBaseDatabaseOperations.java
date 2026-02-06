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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestOceanBaseDatabaseOperations extends TestOceanBase {

  @Test
  public void testBaseOperationDatabase() {
    String databaseName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    // OceanBase database creation does not support incoming comments.
    String comment = null;
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    ((OceanBaseDatabaseOperations) DATABASE_OPERATIONS)
        .createSysDatabaseNameSet()
        .forEach(
            sysOceanBaseDatabaseName ->
                Assertions.assertFalse(databases.contains(sysOceanBaseDatabaseName)));
    testBaseOperation(databaseName, properties, comment);
  }

  @Test
  void testDropDatabaseWithSpecificName() {
    String databaseName = RandomNameUtils.genRandomName("ct_db") + "-abc-" + "end";
    Map<String, String> properties = new HashMap<>();
    DATABASE_OPERATIONS.create(databaseName, null, properties);
    Assertions.assertTrue(DATABASE_OPERATIONS.delete(databaseName, false));
  }
}
