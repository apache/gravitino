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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestViewChange {

  @Test
  public void testReplaceView() {
    Column[] columns = {Column.of("id", Types.IntegerType.get(), "id")};
    Representation[] representations = {
      SQLRepresentation.builder().withDialect("trino").withSql("select id").build()
    };

    ViewChange.ReplaceView replaceView =
        (ViewChange.ReplaceView)
            ViewChange.replaceView(
                columns, representations, "test_catalog", "test_schema", "test_comment");

    assertArrayEquals(columns, replaceView.getColumns());
    assertArrayEquals(representations, replaceView.getRepresentations());
    assertEquals("test_catalog", replaceView.getDefaultCatalog());
    assertEquals("test_schema", replaceView.getDefaultSchema());
    assertEquals("test_comment", replaceView.getComment());
  }

  @Test
  public void testReplaceViewDefensivelyCopiesArrays() {
    Column[] columns = {Column.of("id", Types.IntegerType.get(), "id")};
    Representation[] representations = {
      SQLRepresentation.builder().withDialect("trino").withSql("select id").build()
    };

    ViewChange.ReplaceView replaceView =
        (ViewChange.ReplaceView) ViewChange.replaceView(columns, representations, null, null, null);

    columns[0] = Column.of("name", Types.StringType.get(), "name");
    representations[0] =
        SQLRepresentation.builder().withDialect("spark").withSql("select name").build();

    assertEquals("id", replaceView.getColumns()[0].name());
    assertEquals("trino", ((SQLRepresentation) replaceView.getRepresentations()[0]).dialect());

    replaceView.getColumns()[0] = Column.of("age", Types.IntegerType.get(), "age");
    replaceView.getRepresentations()[0] =
        SQLRepresentation.builder().withDialect("hive").withSql("select age").build();

    assertEquals("id", replaceView.getColumns()[0].name());
    assertEquals("trino", ((SQLRepresentation) replaceView.getRepresentations()[0]).dialect());
  }

  @Test
  public void testReplaceViewAllowsNullDefaultsAndComment() {
    ViewChange.ReplaceView replaceView =
        (ViewChange.ReplaceView)
            ViewChange.replaceView(
                new Column[] {Column.of("id", Types.IntegerType.get(), "id")},
                new Representation[] {
                  SQLRepresentation.builder().withDialect("trino").withSql("select id").build()
                },
                null,
                null,
                null);

    assertNull(replaceView.getDefaultCatalog());
    assertNull(replaceView.getDefaultSchema());
    assertNull(replaceView.getComment());
  }

  @Test
  public void testReplaceViewRejectsNullColumns() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ViewChange.replaceView(
                    null,
                    new Representation[] {
                      SQLRepresentation.builder().withDialect("trino").withSql("select id").build()
                    },
                    null,
                    null,
                    null));

    assertEquals("columns must not be null", exception.getMessage());
  }

  @Test
  public void testReplaceViewRejectsEmptyRepresentations() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ViewChange.replaceView(
                    new Column[] {Column.of("id", Types.IntegerType.get(), "id")},
                    new Representation[0],
                    null,
                    null,
                    null));

    assertEquals("representations must not be null or empty", exception.getMessage());
  }
}
