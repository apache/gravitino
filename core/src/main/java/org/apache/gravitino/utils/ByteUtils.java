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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Utility class containing methods to convert between primitive types and byte arrays. */
public class ByteUtils {

  /**
   * Converts an integer value to a byte array representation.
   *
   * @param v The integer value to convert.
   * @return A byte array representation of the integer value.
   */
  public static byte[] intToByte(int v) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(v);
    return buffer.array();
  }

  /**
   * Converts a long value to a byte array representation.
   *
   * @param v The long value to convert.
   * @return A byte array representation of the long value.
   */
  public static byte[] longToByte(long v) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(v);
    return buffer.array();
  }

  /**
   * Converts a byte array to an integer value.
   *
   * @param bytes The byte array to convert.
   * @return The integer value obtained from the byte array.
   */
  public static int byteToInt(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getInt();
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes The byte array to convert.
   * @return The long value obtained from the byte array.
   */
  public static long byteToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getLong();
  }

  /**
   * Format a byte array to a human-readable string. For example, if the byte array is [0x00, 0x01,
   * 0x02, 0x03], the result is '0x00010203'
   *
   * @param bytes Bytes to be formatted
   * @return A human-readable string
   */
  public static String formatByteArray(byte[] bytes) {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  private ByteUtils() {}
}
