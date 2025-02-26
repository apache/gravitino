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

package org.apache.gravitino.cli.outputs;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;

public class LineUtil {
  // This expression is primarily used to match characters that have a display width of
  // 2, such as characters from Korean, Chinese
  private static final Pattern FULL_WIDTH_PATTERN =
      Pattern.compile(
          "[\u1100-\u115F\u2E80-\uA4CF\uAC00-\uD7A3\uF900-\uFAFF\uFE10-\uFE19\uFE30-\uFE6F\uFF00-\uFF60\uFFE0-\uFFE6]");

  /**
   * Get the display width of a string.
   *
   * @param str the string to measure.
   * @return the display width of the string.
   */
  public static int getDisplayWidth(String str) {
    int width = 0;
    for (int i = 0; i < str.length(); i++) {
      width += getCharWidth(str.charAt(i));
    }

    return width;
  }

  private static int getCharWidth(char ch) {
    String s = String.valueOf(ch);
    if (FULL_WIDTH_PATTERN.matcher(s).find()) {
      return 2;
    }

    return 1;
  }

  /**
   * Get the space string of the specified length.
   *
   * @param n the length of the space string to get.
   * @return the space string of the specified length.
   */
  public static String getSpaces(int n) {
    Preconditions.checkArgument(n >= 0, "n must be non-negative");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      sb.append(' ');
    }
    return sb.toString();
  }

  /**
   * Capitalize the first letter of a string.
   *
   * @param str the string to capitalize.
   * @return the capitalized string.
   */
  public static String capitalize(String str) {
    int strLen = str.length();
    if (strLen == 0) {
      return str;
    } else {
      int firstCodepoint = str.codePointAt(0);
      int newCodePoint = Character.toTitleCase(firstCodepoint);
      if (firstCodepoint == newCodePoint) {
        return str;
      } else {
        int[] newCodePoints = new int[strLen];
        int outOffset = 0;
        newCodePoints[outOffset++] = newCodePoint;

        int codePoint;
        for (int inOffset = Character.charCount(firstCodepoint);
            inOffset < strLen;
            inOffset += Character.charCount(codePoint)) {
          codePoint = str.codePointAt(inOffset);
          newCodePoints[outOffset++] = codePoint;
        }

        return new String(newCodePoints, 0, outOffset);
      }
    }
  }

  /**
   * Get the default value of a column.
   *
   * @param defaultValue the default value expression.
   * @return the default value as a string.
   */
  public static String getDefaultValue(Expression defaultValue) {
    if (defaultValue == null
        || defaultValue == org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET) {
      return "N/A";
    }

    if (defaultValue instanceof Literal && ((Literal<?>) defaultValue).value() != null) {
      return ((Literal<?>) defaultValue).value().toString();
    } else if (defaultValue instanceof FunctionExpression) {
      return defaultValue.toString();
    } else if (defaultValue.references().length == 0) {
      return "N/A";
    }

    return Arrays.toString(defaultValue.references());
  }
}
