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
   * <p>The spec declares the mode and behavior fields case insensitive and accepts both the
   * PascalCase and the snake_case spelling of a value, so {@code EXIST_OK}, {@code ExistOk}, {@code
   * exist_ok} and {@code existok} all normalize to {@code EXISTOK}: a token is compared as its
   * upper-cased letters, with separating underscores carrying no meaning.
   *
   * <p>Authorization relies on this: a mode that reaches the operation as {@code OVERWRITE} has to
   * be recognized as an overwrite while the request is being authorized, whatever spacing,
   * separators or case the client sent.
   *
   * @param value the raw token, may be null
   * @return the trimmed, upper-cased token without underscores, or an empty string when the value
   *     is null
   */
  public static String normalizeToken(String value) {
    return value == null ? "" : value.trim().replace("_", "").toUpperCase(Locale.ROOT);
  }

  static <E extends Enum<E>> E parseEnumToken(
      Class<E> enumClass, String value, String errorMessagePrefix, String instance) {
    String token = normalizeToken(value);
    for (E candidate : enumClass.getEnumConstants()) {
      if (normalizeToken(candidate.name()).equals(token)) {
        return candidate;
      }
    }
    throw new InvalidInputException(
        errorMessagePrefix + value, formatCurrentStackTrace(), instance);
  }
}
