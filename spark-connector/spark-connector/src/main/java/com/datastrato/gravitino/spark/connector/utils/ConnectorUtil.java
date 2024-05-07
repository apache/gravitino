/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.utils;

import static com.datastrato.gravitino.spark.connector.ConnectorConstants.COMMA;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class ConnectorUtil {

  public static String removeDuplicateSparkExtensions(
      String[] extensions, String[] addedExtensions) {
    Set<String> uniqueElements = new LinkedHashSet<>(Arrays.asList(extensions));
    if (addedExtensions != null && addedExtensions.length > 0) {
      uniqueElements.addAll(Arrays.asList(addedExtensions));
    }
    return uniqueElements.stream()
        .reduce((element1, element2) -> element1 + COMMA + element2)
        .orElse("");
  }
}
