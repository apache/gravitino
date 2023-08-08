/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.util;

// Referred from org.eclipse.jetty.util.StringUtil.java
public class StringUtils {

  /**
   * Test if a string is null or only has whitespace characters in it.
   *
   * <p>Note: uses codepoint version of {@link Character#isWhitespace(int)} to support Unicode
   * better.
   *
   * <pre>
   *   isBlank(null)   == true
   *   isBlank("")     == true
   *   isBlank("\r\n") == true
   *   isBlank("\t")   == true
   *   isBlank("   ")  == true
   *   isBlank("a")    == false
   *   isBlank(".")    == false
   *   isBlank(";\n")  == false
   * </pre>
   *
   * @param str the string to test.
   * @return true if string is null or only whitespace characters, false if non-whitespace
   *     characters encountered.
   */
  public static boolean isBlank(String str) {
    if (str == null) {
      return true;
    }

    int len = str.length();
    for (int i = 0; i < len; i++) {
      if (!Character.isWhitespace(str.codePointAt(i))) {
        // found a non-whitespace, we can stop searching  now
        return false;
      }
    }
    // only whitespace
    return true;
  }

  /**
   * Checks if a String is empty ("") or null.
   *
   * <pre>
   *   isEmpty(null)   == true
   *   isEmpty("")     == true
   *   isEmpty("\r\n") == false
   *   isEmpty("\t")   == false
   *   isEmpty("   ")  == false
   *   isEmpty("a")    == false
   *   isEmpty(".")    == false
   *   isEmpty(";\n")  == false
   * </pre>
   *
   * @param str the string to test.
   * @return true if string is null or empty.
   */
  public static boolean isEmpty(String str) {
    return str == null || str.isEmpty();
  }

  /**
   * Test if a string is not null and contains at least 1 non-whitespace characters in it.
   *
   * <p>Note: uses codepoint version of {@link Character#isWhitespace(int)} to support Unicode
   * better.
   *
   * <pre>
   *   isNotBlank(null)   == false
   *   isNotBlank("")     == false
   *   isNotBlank("\r\n") == false
   *   isNotBlank("\t")   == false
   *   isNotBlank("   ")  == false
   *   isNotBlank("a")    == true
   *   isNotBlank(".")    == true
   *   isNotBlank(";\n")  == true
   * </pre>
   *
   * @param str the string to test.
   * @return true if string is not null and has at least 1 non-whitespace character, false if null
   *     or all-whitespace characters.
   */
  public static boolean isNotBlank(String str) {
    if (str == null) {
      return false;
    }

    int len = str.length();
    for (int i = 0; i < len; i++) {
      if (!Character.isWhitespace(str.codePointAt(i))) {
        // found a non-whitespace, we can stop searching  now
        return true;
      }
    }
    // only whitespace
    return false;
  }
}
