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
package org.apache.gravitino.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestSQLRepresentation {

  @Test
  public void testBuildSqlRepresentation() {
    SQLRepresentation representation =
        SQLRepresentation.builder().withDialect("trino").withSql("select 1").build();

    assertEquals(Representation.TYPE_SQL, representation.type());
    assertEquals("trino", representation.dialect());
    assertEquals("select 1", representation.sql());
  }

  @Test
  public void testSqlRepresentationEqualsAndHashCode() {
    SQLRepresentation representation1 =
        SQLRepresentation.builder().withDialect("trino").withSql("select 1").build();
    SQLRepresentation representation2 =
        SQLRepresentation.builder().withDialect("trino").withSql("select 1").build();
    SQLRepresentation representation3 =
        SQLRepresentation.builder().withDialect("spark").withSql("select 1").build();

    assertEquals(representation1, representation2);
    assertEquals(representation1.hashCode(), representation2.hashCode());
    assertNotEquals(representation1, representation3);
  }

  @Test
  public void testBuildSqlRepresentationRejectsEmptyDialect() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SQLRepresentation.builder().withDialect("").withSql("select 1").build());

    assertEquals("dialect must not be null or empty", exception.getMessage());
  }

  @Test
  public void testBuildSqlRepresentationRejectsEmptySql() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SQLRepresentation.builder().withDialect("trino").withSql("").build());

    assertEquals("sql must not be null or empty", exception.getMessage());
  }
}
