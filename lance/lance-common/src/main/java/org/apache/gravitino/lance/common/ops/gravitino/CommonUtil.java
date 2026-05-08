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
class CommonUtil {

  private CommonUtil() {}

  static String formatCurrentStackTrace() {
    return Throwables.getStackTraceAsString(new RuntimeException("Captured stacktrace"));
  }

  static String normalizeToken(String value) {
    return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
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
