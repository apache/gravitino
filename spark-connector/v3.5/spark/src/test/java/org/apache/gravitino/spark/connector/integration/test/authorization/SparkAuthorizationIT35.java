/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.integration.test.authorization;

import static org.junit.Assert.assertThrows;

import org.apache.gravitino.exceptions.ForbiddenException;

/**
 * Spark 3.5+ specific authorization integration tests. Overrides assertion methods for
 * functionality that behaves differently in Spark 3.5+, such as the loadTable method with
 * TableWritePrivilege support.
 */
public class SparkAuthorizationIT35 extends SparkAuthorizationIT {

  /**
   * In Spark 3.5+, INSERT should throw ForbiddenException when user doesn't have MODIFY_TABLE
   * privilege because Spark 3.5+ supports {@code loadTable(Identifier, Set<TableWritePrivilege>)}.
   */
  @Override
  protected void assertInsertBehaviorWithoutModifyPrivilege(String tableName) {
    assertThrows(
        ForbiddenException.class,
        () -> getSparkSession().sql(String.format("INSERT INTO %s VALUES (1, 'test')", tableName)));
  }
}
