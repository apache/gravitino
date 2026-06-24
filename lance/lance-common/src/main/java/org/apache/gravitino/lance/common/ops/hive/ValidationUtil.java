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
package org.apache.gravitino.lance.common.ops.hive;

import com.google.errorprone.annotations.FormatMethod;
import org.lance.namespace.errors.InvalidInputException;

/** Validation helpers that surface failures as Lance {@link InvalidInputException}s. */
class ValidationUtil {

  private ValidationUtil() {}

  @FormatMethod
  static void checkArgument(boolean expression, String message, Object... args) {
    if (!expression) {
      throw new InvalidInputException(String.format(message, args));
    }
  }

  static String checkNotNullOrEmptyString(String value, String message) {
    if (value == null || value.isEmpty()) {
      throw new InvalidInputException(message);
    }
    return value;
  }
}
