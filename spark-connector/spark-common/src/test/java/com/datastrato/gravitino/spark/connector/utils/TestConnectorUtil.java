/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.utils;

import static com.datastrato.gravitino.spark.connector.ConnectorConstants.COMMA;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestConnectorUtil {

  @Test
  void testRemoveDuplicateSparkExtensions() {
    String[] extensions = {"a", "b", "c"};
    String addedExtensions = "a,d,e";
    String result =
        ConnectorUtil.removeDuplicateSparkExtensions(extensions, addedExtensions.split(COMMA));
    Assertions.assertEquals(result, "a,b,c,d,e");

    extensions = new String[] {"a", "a", "b", "c"};
    addedExtensions = "";
    result = ConnectorUtil.removeDuplicateSparkExtensions(extensions, addedExtensions.split(COMMA));
    Assertions.assertEquals(result, "a,b,c");

    extensions = new String[] {"a", "a", "b", "c"};
    addedExtensions = "b";
    result = ConnectorUtil.removeDuplicateSparkExtensions(extensions, addedExtensions.split(COMMA));
    Assertions.assertEquals(result, "a,b,c");

    extensions = new String[] {"a", "a", "b", "c"};
    result = ConnectorUtil.removeDuplicateSparkExtensions(extensions, null);
    Assertions.assertEquals(result, "a,b,c");
  }
}
