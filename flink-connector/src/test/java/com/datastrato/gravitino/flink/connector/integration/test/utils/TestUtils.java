/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.integration.test.utils;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;

public class TestUtils {

  public static void assertTableResult(
      TableResult actual, ResultKind expectedResultKind, Row... expected) {
    ResultKind actualRowKind = actual.getResultKind();
    Assertions.assertEquals(expectedResultKind, actualRowKind);

    if (actualRowKind == ResultKind.SUCCESS_WITH_CONTENT) {
      List<Row> actualRows = Lists.newArrayList(actual.collect());
      Assertions.assertEquals(expected.length, actualRows.size());
      for (int i = 0; i < expected.length; i++) {
        Row expectedRow = expected[i];
        Row actualRow = actualRows.get(i);
        Assertions.assertEquals(expectedRow.getKind(), actualRow.getKind());
        Assertions.assertEquals(expectedRow, actualRow);
      }
    }
  }
}
