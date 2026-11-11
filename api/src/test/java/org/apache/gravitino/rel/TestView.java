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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.junit.jupiter.api.Test;

public class TestView {

  @Test
  public void testSqlForReturnsMatchingSqlRepresentation() {
    SQLRepresentation trinoRepresentation =
        SQLRepresentation.builder().withDialect("trino").withSql("select 1").build();
    SQLRepresentation sparkRepresentation =
        SQLRepresentation.builder().withDialect("spark").withSql("select 2").build();

    View view =
        new View() {
          @Override
          public String name() {
            return "test_view";
          }

          @Override
          public Column[] columns() {
            return new Column[0];
          }

          @Override
          public Representation[] representations() {
            return new Representation[] {trinoRepresentation, sparkRepresentation};
          }

          @Override
          public Audit auditInfo() {
            return testAudit();
          }
        };

    Optional<SQLRepresentation> sqlRepresentation = view.sqlFor("TRINO");

    assertTrue(sqlRepresentation.isPresent());
    assertEquals(trinoRepresentation, sqlRepresentation.get());
  }

  @Test
  public void testSqlForReturnsEmptyWhenDialectIsMissing() {
    View view =
        testView(SQLRepresentation.builder().withDialect("spark").withSql("select 1").build());

    assertFalse(view.sqlFor("trino").isPresent());
  }

  @Test
  public void testSqlForReturnsEmptyWhenDialectIsNull() {
    View view =
        testView(SQLRepresentation.builder().withDialect("trino").withSql("select 1").build());

    assertFalse(view.sqlFor(null).isPresent());
  }

  @Test
  public void testSqlForReturnsEmptyWhenRepresentationIsNotSql() {
    View view =
        testView(
            new Representation() {
              @Override
              public String type() {
                return "custom";
              }
            });

    assertFalse(view.sqlFor("trino").isPresent());
  }

  @Test
  public void testDefaultCatalogAndSchemaDefaultToNull() {
    View view =
        testView(SQLRepresentation.builder().withDialect("trino").withSql("select 1").build());

    assertNull(view.defaultCatalog());
    assertNull(view.defaultSchema());
  }

  private static View testView(Representation... representations) {
    return new View() {
      @Override
      public String name() {
        return "test_view";
      }

      @Override
      public Column[] columns() {
        return new Column[0];
      }

      @Override
      public Representation[] representations() {
        return representations;
      }

      @Override
      public Audit auditInfo() {
        return testAudit();
      }
    };
  }

  private static Audit testAudit() {
    return new Audit() {
      @Override
      public String creator() {
        return "test";
      }

      @Override
      public Instant createTime() {
        return Instant.EPOCH;
      }

      @Override
      public String lastModifier() {
        return "test";
      }

      @Override
      public Instant lastModifiedTime() {
        return Instant.EPOCH;
      }
    };
  }
}
