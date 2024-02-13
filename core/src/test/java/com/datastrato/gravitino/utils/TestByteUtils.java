/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestByteUtils {

  @Test
  void testIntToByte() {
    int v = 258;
    byte[] b = ByteUtils.intToByte(v);
    Assertions.assertArrayEquals(new byte[] {0x00, 0x00, 0x01, 0x02}, b);
    int v2 = ByteUtils.byteToInt(b);
    assert v == v2;
  }

  @Test
  void testLongToByte() {
    long v = 259;
    byte[] b = ByteUtils.longToByte(v);
    Assertions.assertArrayEquals(new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03}, b);
    long v2 = ByteUtils.byteToLong(b);
    assert v == v2;
  }

  @Test
  void testFormatByteArray() {
    byte[] b = new byte[] {0x00, 0x01, 0x02, 0x03};
    String s = ByteUtils.formatByteArray(b);
    Assertions.assertEquals("0x00010203", s);
  }
}
