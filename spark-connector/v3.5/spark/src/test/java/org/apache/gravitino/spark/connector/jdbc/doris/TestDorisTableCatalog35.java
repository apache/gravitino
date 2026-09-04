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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
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
    assertTrue(result.getMessage().contains("RuntimeException"));
    assertNull(result.getCause());
  }

  @Test
  void testSqlFailureExposesOnlySafeDiagnosticFields() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    SQLException failure =
        new SQLException("password=secret-token: authorization failed", "28000", 1045);

    IllegalArgumentException result =
        DorisTableCatalog35.physicalSchemaLoadFailure(identifier, failure);

    assertTrue(result.getMessage().contains("SQLException"));
    assertTrue(result.getMessage().contains("SQLState=28000"));
    assertTrue(result.getMessage().contains("errorCode=1045"));
    assertFalse(result.getMessage().contains("secret-token"));
    assertNull(result.getCause());

    SQLException unsafeState = new SQLException("authorization failed", "secret-token", 1045);
    IllegalArgumentException unsafeStateResult =
        DorisTableCatalog35.physicalSchemaLoadFailure(identifier, unsafeState);
    assertTrue(unsafeStateResult.getMessage().contains("SQLState=unknown"));
    assertFalse(unsafeStateResult.getMessage().contains("secret-token"));
  }

  @Test
  void testPhysicalSchemaValidationPreservesDriftDetails() {
    Schema schema = new Schema(0, "DUP_KEYS", List.of(new Field("id", "INT", "", 0, 0, "NONE")));

    IllegalArgumentException countFailure =
        assertThrows(
            IllegalArgumentException.class,
            () -> DorisTableCatalog35.buildPhysicalSchema(schema, List.of()));
    assertEquals("Doris FE and JDBC column counts differ: FE=1, JDBC=0", countFailure.getMessage());

    IllegalArgumentException metadataFailure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisTableCatalog35.buildPhysicalSchema(
                    schema,
                    List.of(new DorisTableCatalog35.JdbcColumnMetadata("id", "BIGINT", true))));
    assertTrue(metadataFailure.getMessage().contains("index 0"));
    assertTrue(metadataFailure.getMessage().contains("FE=id INT"));
    assertTrue(metadataFailure.getMessage().contains("JDBC=id BIGINT"));
  }
}
