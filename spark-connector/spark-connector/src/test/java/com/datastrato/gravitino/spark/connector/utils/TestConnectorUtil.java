/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestConnectorUtil {

  @Test
  void testRemoveDuplicates() {
    String[] elements = {"a", "b", "c"};
    String otherElements = "a,d,e";
    String result = ConnectorUtil.removeDuplicates(elements, otherElements);
    Assertions.assertEquals(result, "a,b,c,d,e");

    elements = new String[] {"a", "a", "b", "c"};
    otherElements = "";
    result = ConnectorUtil.removeDuplicates(elements, otherElements);
    Assertions.assertEquals(result, "a,b,c");

    elements = new String[] {"a", "a", "b", "c"};
    result = ConnectorUtil.removeDuplicates(elements, null);
    Assertions.assertEquals(result, "a,b,c");
  }
}
