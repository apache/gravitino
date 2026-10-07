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
   * the client sent.
   *
   * @param value the raw token, may be null
   * @return the trimmed, upper-cased token, or an empty string when the value is null
   */
  public static String normalizeToken(String value) {
    return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
  }

  /**
   * Resolves a request token to a constant of {@code enumClass}, accepting the two spellings the
   * Lance namespace specification uses for the same value: snake_case ({@code exist_ok}) and
   * PascalCase ({@code ExistOk}).
   *
   * <p>A token matches a constant when it equals the constant name or that name with its
   * underscores removed, and nothing looser. The same token is also read as a mode by {@code
   * LanceMetadataAuthorizationMethodInterceptor} through {@link #normalizeToken}, which compares it
   * against the literal {@code OVERWRITE}; a spelling resolved to {@code OVERWRITE} here but not
   * recognized there would be authorized as a plain create, letting a caller holding only a create
   * privilege replace an object somebody else owns.
   *
   * @param <E> the enum type being resolved
   * @param enumClass the enum whose constants the token is resolved against
   * @param value the raw token sent by the client
   * @param errorMessagePrefix the prefix of the message reported when no constant matches
   * @param instance the identifier the error is reported for
   * @return the matching constant
   * @throws InvalidInputException when no constant matches the token
   */
  static <E extends Enum<E>> E parseEnumToken(
      Class<E> enumClass, String value, String errorMessagePrefix, String instance) {
    String token = normalizeToken(value);
    for (E constant : enumClass.getEnumConstants()) {
      String name = constant.name();
      if (name.equals(token) || name.replace("_", "").equals(token)) {
        return constant;
      }
    }
    throw new InvalidInputException(
        errorMessagePrefix + value, formatCurrentStackTrace(), instance);
  }
}
