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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestByteUtils {

  @Test
  public void testIntToByte() {
    int v = 258;
    byte[] b = ByteUtils.intToByte(v);
    Assertions.assertArrayEquals(new byte[] {0x00, 0x00, 0x01, 0x02}, b);
    int v2 = ByteUtils.byteToInt(b);
    Assertions.assertEquals(v, v2);
  }

  @Test
  public void testLongToByte() {
    long v = 259;
    byte[] b = ByteUtils.longToByte(v);
    Assertions.assertArrayEquals(new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03}, b);
    long v2 = ByteUtils.byteToLong(b);
    Assertions.assertEquals(v, v2);
  }

  @Test
  public void testFormatByteArray() {
    byte[] b = new byte[] {0x00, 0x01, 0x02, 0x03};
    String s = ByteUtils.formatByteArray(b);
    Assertions.assertEquals("0x00010203", s);
  }
}
