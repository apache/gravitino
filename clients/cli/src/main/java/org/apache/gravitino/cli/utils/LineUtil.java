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

package org.apache.gravitino.cli.utils;

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

public class LineUtil {
  // This expression is primarily used to match characters that have a display width of
  // 2, such as characters from Korean, Chinese
  private static final Pattern FULL_WIDTH_PATTERN =
      Pattern.compile(
          "[\u1100-\u115F\u2E80-\uA4CF\uAC00-\uD7A3\uF900-\uFAFF\uFE10-\uFE19\uFE30-\uFE6F\uFF00-\uFF60\uFFE0-\uFFE6]");

  /**
   * Calculates the display width of a string, considering full-width characters. Full-width
   * characters are counted as width 2, others as width 1.
   *
   * @param str Input string to measure
   * @return Display width of the string, 0 if input is null
   */
  public static int getDisplayWidth(String str) {
    if (str == null) return 0;
    int width = 0;
    for (int i = 0; i < str.length(); i++) {
      width += getCharWidth(str.charAt(i));
    }

    return width;
  }

  /**
   * Determines the display width of a single character.
   *
   * @param ch Character to measure
   * @return 2 for full-width characters, 1 for others
   */
  private static int getCharWidth(char ch) {
    String s = String.valueOf(ch);
    if (FULL_WIDTH_PATTERN.matcher(s).find()) {
      return 2;
    }

    return 1;
  }

  /**
   * Creates a string containing a specified number of spaces.
   *
   * @param n Number of spaces to generate
   * @return String containing n spaces
   * @throws IllegalArgumentException if n is negative
   */
  public static String getSpaces(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      sb.append(' ');
    }
    return sb.toString();
  }

  /**
   * Adds an element to the beginning of an array.
   *
   * @param element Element to add at the start
   * @param array Original array
   * @param <T> Type of array elements
   * @return New array with element added at the beginning
   * @throws NullPointerException if array is null
   */
  public static <T> T[] addFirst(T element, T[] array) {
    Objects.requireNonNull(array, "Array must not be null");
    @SuppressWarnings("unchecked")
    T[] newArray = Arrays.copyOf(array, array.length + 1);
    System.arraycopy(array, 0, newArray, 1, array.length);
    newArray[0] = element;
    return newArray;
  }

  /**
   * Creates a new array filled with the specified string value.
   *
   * @param str String to fill the array with
   * @param length Length of the array to create
   * @return New array filled with the specified string
   * @throws IllegalArgumentException if str is null or length is not positive
   */
  public static String[] buildArrayWithFill(String str, int length) {
    if (str == null) {
      throw new IllegalArgumentException("str must not be null");
    }
    if (length <= 0) {
      throw new IllegalArgumentException("length must be greater than 0");
    }

    String[] result = new String[length];
    Arrays.fill(result, str);
    return result;
  }
}
