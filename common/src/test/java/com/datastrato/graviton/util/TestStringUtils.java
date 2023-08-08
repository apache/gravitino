/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringUtils {

  @Test
  public void testIsBlank() {
    Assertions.assertTrue(StringUtils.isBlank(null));
    Assertions.assertTrue(StringUtils.isBlank(""));
    Assertions.assertTrue(StringUtils.isBlank("\r\n"));
    Assertions.assertTrue(StringUtils.isBlank("\t"));
    Assertions.assertTrue(StringUtils.isBlank("    "));
    Assertions.assertTrue(StringUtils.isBlank(" \r\n\t"));
    Assertions.assertFalse(StringUtils.isBlank("a"));
    Assertions.assertFalse(StringUtils.isBlank("."));
    Assertions.assertFalse(StringUtils.isBlank(";\n"));
  }

  @Test
  public void testIsEmpty() {
    Assertions.assertTrue(StringUtils.isEmpty(null));
    Assertions.assertTrue(StringUtils.isEmpty(""));
    Assertions.assertFalse(StringUtils.isEmpty("\r\n"));
    Assertions.assertFalse(StringUtils.isEmpty("\t"));
    Assertions.assertFalse(StringUtils.isEmpty("    "));
    Assertions.assertFalse(StringUtils.isEmpty(" \r\n\t"));
    Assertions.assertFalse(StringUtils.isEmpty("a"));
    Assertions.assertFalse(StringUtils.isEmpty("."));
    Assertions.assertFalse(StringUtils.isEmpty(";\n"));
  }

  @Test
  public void testIsNotBlank() {
    Assertions.assertFalse(StringUtils.isNotBlank(null));
    Assertions.assertFalse(StringUtils.isNotBlank(""));
    Assertions.assertFalse(StringUtils.isNotBlank("\r\n"));
    Assertions.assertFalse(StringUtils.isNotBlank("\t"));
    Assertions.assertFalse(StringUtils.isNotBlank("    "));
    Assertions.assertFalse(StringUtils.isNotBlank(" \r\n\t"));
    Assertions.assertTrue(StringUtils.isNotBlank("a"));
    Assertions.assertTrue(StringUtils.isNotBlank("."));
    Assertions.assertTrue(StringUtils.isNotBlank(";\n"));
  }
}
