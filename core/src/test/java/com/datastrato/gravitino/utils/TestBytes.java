/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestBytes {

  @Test
  void testWrapAndGet() {
    byte[] data = {0x01, 0x02, 0x03};
    Bytes bytes = Bytes.wrap(data);

    assertArrayEquals(data, bytes.get());
  }

  @Test
  void testCompareTo() {
    byte[] data1 = {0x01, 0x02, 0x03};
    byte[] data2 = {0x01, 0x02, 0x03};
    byte[] data3 = {0x01, 0x02, 0x04};
    byte[] data4 = {0x01, 0x02};

    Bytes bytes1 = new Bytes(data1);
    Bytes bytes3 = new Bytes(data3);
    Bytes bytes4 = new Bytes(data4);

    assertEquals(0, bytes1.compareTo(data2));
    assertTrue(bytes1.compareTo(data3) < 0);
    assertTrue(bytes3.compareTo(data1) > 0);
    assertTrue(bytes1.compareTo(data2) == 0);

    // Test cases for different lengths
    assertTrue(bytes1.compareTo(data4) > 0);
    assertTrue(bytes4.compareTo(data1) < 0);
  }

  @Test
  void testConcat() {
    byte[] first = {0x01, 0x02};
    byte[] second = {0x03, 0x04};
    byte[] expected = {0x01, 0x02, 0x03, 0x04};
    byte[] result = Bytes.concat(first, second);

    assertArrayEquals(expected, result);
  }

  @Test
  void testHashCode() {
    byte[] data1 = {0x01, 0x02, 0x03};
    byte[] data2 = {0x01, 0x02, 0x03};
    Bytes bytes1 = new Bytes(data1);
    Bytes bytes2 = new Bytes(data2);

    assertEquals(bytes1.hashCode(), bytes2.hashCode());
  }

  @Test
  void testEquals() {
    byte[] data1 = {0x01, 0x02, 0x03};
    byte[] data2 = {0x01, 0x02, 0x03};
    Bytes bytes1 = new Bytes(data1);
    Bytes bytes2 = new Bytes(data2);

    assertTrue(bytes1.equals(bytes2));
  }

  @Test
  void testIncrement() {
    byte[] data = {0x01, 0x01};
    byte[] expected = {0x01, 0x02};
    Bytes bytes = Bytes.increment(new Bytes(data));

    assertArrayEquals(expected, bytes.get());
  }

  @Test
  void testIncrementCarry() {
    byte[] data = {0x01, (byte) 0xFF};
    byte[] expected = {0x02, 0x00};
    Bytes bytes = Bytes.increment(new Bytes(data));

    assertArrayEquals(expected, bytes.get());
  }

  @Test
  void testToString() {
    byte[] data = {0x01, 0x02, (byte) 0xAB};
    String expected = "\\x01\\x02\\xAB";
    Bytes bytes = new Bytes(data);
    String result = bytes.toString();

    assertEquals(expected, result);
  }

  @Test
  void testLexicographicByteArrayComparator() {
    byte[] data1 = {0x01, 0x02, 0x03};
    byte[] data2 = {0x01, 0x02, 0x04};
    byte[] data3 = {0x01, 0x02, 0x03, 0x00};
    Bytes.ByteArrayComparator comparator = Bytes.BYTES_LEXICO_COMPARATOR;

    assertTrue(comparator.compare(data1, data2) < 0);
    assertTrue(comparator.compare(data2, data1) > 0);
    assertTrue(comparator.compare(data1, data1) == 0);
    assertTrue(comparator.compare(data1, data3) < 0);
  }
}
