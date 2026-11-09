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
package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.base.Throwables;
import java.util.Locale;
import org.lance.namespace.errors.InvalidInputException;

/** Utility methods used by Gravitino Lance namespace operations. */
public class CommonUtil {

  private CommonUtil() {}

  static String formatCurrentStackTrace() {
    return Throwables.getStackTraceAsString(new RuntimeException("Captured stacktrace"));
  }

  /**
   * Normalizes a request token the way every mode and behavior parameter is read, so that callers
   * deciding something from a token compare it exactly as the operation that acts on it will.
   *
   * <p>Authorization relies on this: a mode that reaches the operation as {@code OVERWRITE} has to
   * be recognized as an overwrite while the request is being authorized, whatever spacing or case
   * the client sent. A single-word token is not split on internal capitals, so {@code OverWrite}
   * stays {@code OVERWRITE}.
   *
   * <p>{@code ExistOk} is the only multi-word token in the Lance mode vocabulary. The PascalCase
   * form and the underscore-free form are accepted as {@code EXIST_OK}, matching snake_case {@code
   * exist_ok}.
   *
   * @param value the raw token, may be null
   * @return the trimmed, upper-cased token, with {@code ExistOk} mapped to {@code EXIST_OK}, or an
   *     empty string when the value is null
   */
  public static String normalizeToken(String value) {
    if (value == null) {
      return "";
    }
    String normalized = value.trim().toUpperCase(Locale.ROOT);
    // Only ExistOk is multi-word. Splitting every capital would turn OverWrite into OVER_WRITE
    // and authorization would no longer recognize an overwrite.
    if ("EXISTOK".equals(normalized)) {
      return "EXIST_OK";
    }
    return normalized;
  }

  static <E extends Enum<E>> E parseEnumToken(
      Class<E> enumClass, String value, String errorMessagePrefix, String instance) {
    try {
      return Enum.valueOf(enumClass, normalizeToken(value));
    } catch (IllegalArgumentException e) {
      throw new InvalidInputException(
          errorMessagePrefix + value, formatCurrentStackTrace(), instance);
    }
  }
}
