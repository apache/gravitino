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

package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestBytes {

  @Test
  public void testWrapAndGet() {
    byte[] data = {0x01, 0x02, 0x03};
    Bytes bytes = Bytes.wrap(data);

    assertArrayEquals(data, bytes.get());
  }

  @Test
  public void testCompareTo() {
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
  public void testConcat() {
    byte[] first = {0x01, 0x02};
    byte[] second = {0x03, 0x04};
    byte[] expected = {0x01, 0x02, 0x03, 0x04};
    byte[] result = Bytes.concat(first, second);

    assertArrayEquals(expected, result);
  }

  @Test
  public void testHashCode() {
    byte[] data1 = {0x01, 0x02, 0x03};
    byte[] data2 = {0x01, 0x02, 0x03};
    Bytes bytes1 = new Bytes(data1);
    Bytes bytes2 = new Bytes(data2);

    assertEquals(bytes1.hashCode(), bytes2.hashCode());
  }

  @Test
  public void testEquals() {
    byte[] data1 = {0x01, 0x02, 0x03};
    byte[] data2 = {0x01, 0x02, 0x03};
    Bytes bytes1 = new Bytes(data1);
    Bytes bytes2 = new Bytes(data2);

    assertTrue(bytes1.equals(bytes2));
  }

  @Test
  public void testIncrement() {
    byte[] data = {0x01, 0x01};
    byte[] expected = {0x01, 0x02};
    Bytes bytes = Bytes.increment(new Bytes(data));

    assertArrayEquals(expected, bytes.get());
  }

  @Test
  public void testIncrementCarry() {
    byte[] data = {0x01, (byte) 0xFF};
    byte[] expected = {0x02, 0x00};
    Bytes bytes = Bytes.increment(new Bytes(data));

    assertArrayEquals(expected, bytes.get());
  }

  @Test
  public void testToString() {
    byte[] data = {0x01, 0x02, (byte) 0xAB};
    String expected = "\\x01\\x02\\xAB";
    Bytes bytes = new Bytes(data);
    String result = bytes.toString();

    assertEquals(expected, result);
  }

  @Test
  public void testLexicographicByteArrayComparator() {
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
