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
package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

/** Tests error handling at the boundary with the official Doris catalog. */
public class TestDorisTableCatalog35 {

  @Test
  void testPhysicalSchemaFailureDoesNotExposeCauseDetails() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    RuntimeException failure =
        new RuntimeException(
            "jdbc:mysql://fe:9030/db?user=admin&password=secret-token: authorization failed");

    IllegalArgumentException result =
        DorisTableCatalog35.physicalSchemaLoadFailure(identifier, failure);

    assertFalse(result.getMessage().contains("secret-token"));
    assertFalse(result.getMessage().contains("jdbc:mysql"));
    assertNull(result.getCause());
  }
}
