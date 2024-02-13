/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.operation;

import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link PostgreSqlTableOperations}. */
public class TestPostgreSqlTableOperations {

  @Test
  void testAppendIndexesSql() {
    // Test append index sql success.
    Index[] indexes =
        new Index[] {
          Indexes.primary("test_pk", new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}})
        };
    StringBuilder successBuilder = new StringBuilder();
    PostgreSqlTableOperations.appendIndexesSql(indexes, successBuilder);
    Assertions.assertEquals(
        ",\n"
            + "CONSTRAINT \"test_pk\" PRIMARY KEY (\"col_1\", \"col_2\"),\n"
            + "CONSTRAINT \"u1_key\" UNIQUE (\"col_2\", \"col_3\"),\n"
            + "CONSTRAINT \"u2_key\" UNIQUE (\"col_3\", \"col_4\")",
        successBuilder.toString());

    // Test append index sql not have name.
    indexes =
        new Index[] {
          Indexes.primary(null, new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique(null, new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique(null, new String[][] {{"col_3"}, {"col_4"}})
        };
    successBuilder = new StringBuilder();
    PostgreSqlTableOperations.appendIndexesSql(indexes, successBuilder);
    Assertions.assertEquals(
        ",\n"
            + " PRIMARY KEY (\"col_1\", \"col_2\"),\n"
            + " UNIQUE (\"col_2\", \"col_3\"),\n"
            + " UNIQUE (\"col_3\", \"col_4\")",
        successBuilder.toString());

    // Test append index sql failed.
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PostgreSqlTableOperations.appendIndexesSql(
                    new Index[] {
                      Indexes.primary("test_pk", new String[][] {{"col_1", "col_2"}}),
                      Indexes.unique("u1_key", new String[][] {{"col_2", "col_3"}}),
                      Indexes.unique("u2_key", new String[][] {{"col_3", "col_4"}})
                    },
                    new StringBuilder()));
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in PostgreSQL"));
  }
}
