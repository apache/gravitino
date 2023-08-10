/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.util;

import com.datastrato.graviton.util.Bytes.ByteArrayComparator;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class Bytes implements Comparable<byte[]> {

  public static final byte[] EMPTY = new byte[0];

  private static final char[] HEX_CHARS_UPPER = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
  };

  private final byte[] bytes;

  // Cache the hash code for the byte array.
  private int hashCode;

  /**
   * Wraps an existing byte array into a Bytes object.
   *
   * @param bytes The byte array to wrap.
   * @return A new Bytes object wrapping the given byte array.
   */
  public static Bytes wrap(byte[] bytes) {
    if (bytes == null) return null;
    return new Bytes(bytes);
  }

  /**
   * Constructs a Bytes object using the provided byte array.
   *
   * @param bytes The byte array to use as backing storage.
   */
  public Bytes(byte[] bytes) {
    this.bytes = bytes;

    // initialize hash code to 0
    hashCode = 0;
  }

  /**
   * Retrieves the underlying byte array.
   *
   * @return The underlying byte array.
   */
  public byte[] get() {
    return this.bytes;
  }

  /**
   * Concatenates multiple byte arrays into a single byte array.
   *
   * @param values An array of byte arrays to be concatenated.
   * @return A new byte array containing the concatenated data.
   */
  public static byte[] concat(byte[]... values) {
    int totalLen = 0;
    for (byte[] b : values) {
      totalLen += b.length;
    }

    byte[] res = new byte[totalLen];
    int currentPos = 0;
    for (int i = 0; i < values.length; i++) {
      System.arraycopy(values[i], 0, res, currentPos, values[i].length);
      currentPos += values[i].length;
    }
    return res;
  }

  /**
   * Computes the hash code for the byte array.
   *
   * <p>The hashcode is cached except for the case where it is computed as 0, in which case we
   * compute the hashcode on every call.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = Arrays.hashCode(bytes);
    }
    return hashCode;
  }

  /**
   * Checks if this Bytes object is equal to another object.
   *
   * @param other The object to compare with.
   * @return true if the objects are equal, false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null) return false;
    if (this.hashCode() != other.hashCode()) return false;
    if (other instanceof Bytes) return Arrays.equals(this.bytes, ((Bytes) other).get());
    return false;
  }

  /**
   * Compares this Bytes object to a given byte array lexicographically.
   *
   * @param o The byte array to compare with.
   * @return A negative integer, zero, or a positive integer if this object is less than, equal to,
   *     or greater than the specified byte array.
   */
  @Override
  public int compareTo(byte[] o) {
    return BYTES_LEXICO_COMPARATOR.compare(this.bytes, o);
  }

  /**
   * Returns a string representation of the Bytes object.
   *
   * @return A string representation of the byte array.
   */
  @Override
  public String toString() {
    return Bytes.toString(bytes, 0, bytes.length);
  }

  /**
   * Converts a byte array to a printable string, escaping non-printable characters.
   *
   * @param b The byte array to convert.
   * @param off The starting offset within the array.
   * @param len The length of data to convert.
   * @return A printable representation of the byte array.
   */
  private static String toString(final byte[] b, int off, int len) {
    StringBuilder result = new StringBuilder();

    if (b == null) return result.toString();

    // just in case we are passed a 'len' that is > buffer length...
    if (off >= b.length) return result.toString();

    if (off + len > b.length) len = b.length - off;

    for (int i = off; i < off + len; ++i) {
      int ch = b[i] & 0xFF;
      if (ch >= ' ' && ch <= '~' && ch != '\\') {
        result.append((char) ch);
      } else {
        result.append("\\x");
        result.append(HEX_CHARS_UPPER[ch / 0x10]);
        result.append(HEX_CHARS_UPPER[ch % 0x10]);
      }
    }
    return result.toString();
  }

  /**
   * Increments a byte array by 1, throwing an exception on overflow.
   *
   * @param input The byte array to increment.
   * @return A new Bytes object containing the incremented byte array.
   * @throws IndexOutOfBoundsException If incrementing would cause overflow.
   */
  public static Bytes increment(Bytes input) throws IndexOutOfBoundsException {
    byte[] inputArr = input.get();
    byte[] ret = new byte[inputArr.length];
    int carry = 1;
    for (int i = inputArr.length - 1; i >= 0; i--) {
      if (inputArr[i] == (byte) 0xFF && carry == 1) {
        ret[i] = (byte) 0x00;
      } else {
        ret[i] = (byte) (inputArr[i] + carry);
        carry = 0;
      }
    }
    if (carry == 0) {
      return wrap(ret);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /** A byte array comparator based on lexicographic ordering. */
  public static final ByteArrayComparator BYTES_LEXICO_COMPARATOR =
      new LexicographicByteArrayComparator();

  /** Interface for comparing byte arrays lexicographically. */
  public interface ByteArrayComparator extends Comparator<byte[]>, Serializable {

    int compare(
        final byte[] buffer1,
        int offset1,
        int length1,
        final byte[] buffer2,
        int offset2,
        int length2);
  }

  /** A byte array comparator based on lexicographic ordering. */
  private static class LexicographicByteArrayComparator implements ByteArrayComparator {

    @Override
    public int compare(byte[] buffer1, byte[] buffer2) {
      return compare(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
    }

    public int compare(
        final byte[] buffer1,
        int offset1,
        int length1,
        final byte[] buffer2,
        int offset2,
        int length2) {

      // Short circuit equal case
      if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
        return 0;
      }

      // Similar to Arrays.compare() but considers offset and length
      int end1 = offset1 + length1;
      int end2 = offset2 + length2;
      for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
        int a = buffer1[i] & 0xff;
        int b = buffer2[j] & 0xff;
        if (a != b) {
          return a - b;
        }
      }
      return length1 - length2;
    }
  }
}
