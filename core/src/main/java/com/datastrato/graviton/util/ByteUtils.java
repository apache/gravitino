/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteUtils {
  public static byte[] intToByte(int v) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(v);
    return buffer.array();
  }

  public static byte[] longToByte(long v) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(v);
    return buffer.array();
  }

  public static int byteToInt(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getInt();
  }

  public static long byteToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getLong();
  }
}
