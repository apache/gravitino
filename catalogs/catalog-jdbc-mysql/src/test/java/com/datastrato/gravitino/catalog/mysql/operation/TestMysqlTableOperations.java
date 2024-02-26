/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.operation;

import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link MysqlTableOperations}. */
public class TestMysqlTableOperations {

  @Test
  void testAppendIndexesBuilder() {
    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_2"}, {"col_1"}}),
          Indexes.unique("uk_col_4", new String[][] {{"col_4"}}),
          Indexes.unique("uk_col_5", new String[][] {{"col_4"}, {"col_5"}}),
          Indexes.unique("uk_col_6", new String[][] {{"col_4"}, {"col_5"}, {"col_6"}})
        };
    StringBuilder sql = new StringBuilder();
    MysqlTableOperations.appendIndexesSql(indexes, sql);
    String expectedStr =
        ",\n"
            + "CONSTRAINT PRIMARY KEY (`col_2`, `col_1`),\n"
            + "CONSTRAINT `uk_col_4` UNIQUE (`col_4`),\n"
            + "CONSTRAINT `uk_col_5` UNIQUE (`col_4`, `col_5`),\n"
            + "CONSTRAINT `uk_col_6` UNIQUE (`col_4`, `col_5`, `col_6`)";
    Assertions.assertEquals(expectedStr, sql.toString());

    indexes =
        new Index[] {
          Indexes.unique("uk_1", new String[][] {{"col_4"}}),
          Indexes.unique("uk_2", new String[][] {{"col_4"}, {"col_3"}}),
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_2"}, {"col_1"}, {"col_3"}}),
          Indexes.unique("uk_3", new String[][] {{"col_4"}, {"col_5"}, {"col_6"}, {"col_7"}})
        };
    sql = new StringBuilder();
    MysqlTableOperations.appendIndexesSql(indexes, sql);
    expectedStr =
        ",\n"
            + "CONSTRAINT `uk_1` UNIQUE (`col_4`),\n"
            + "CONSTRAINT `uk_2` UNIQUE (`col_4`, `col_3`),\n"
            + "CONSTRAINT PRIMARY KEY (`col_2`, `col_1`, `col_3`),\n"
            + "CONSTRAINT `uk_3` UNIQUE (`col_4`, `col_5`, `col_6`, `col_7`)";
    Assertions.assertEquals(expectedStr, sql.toString());
  }
}
