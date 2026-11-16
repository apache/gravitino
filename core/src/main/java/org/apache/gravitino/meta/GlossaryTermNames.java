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

package org.apache.gravitino.meta;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/** Shared normalization for persisted glossary names and synonyms. */
public final class GlossaryTermNames {
  private static final Pattern SPACE = Pattern.compile("\\s+", Pattern.UNICODE_CHARACTER_CLASS);

  private GlossaryTermNames() {}
  /** Removes surrounding Unicode whitespace while preserving display spelling. */
  public static String display(String value) {
    return SPACE.matcher(Objects.requireNonNull(value, "name")).replaceAll(" ").trim().isEmpty()
        ? ""
        : value.replaceAll("(?U)^\\s+|\\s+$", "");
  }
  /** Returns the locale-independent lookup key. */
  public static String key(String value) {
    return SPACE.matcher(display(value)).replaceAll(" ").toLowerCase(Locale.ROOT);
  }
  /** Checks the persisted 128-code-point bound. */
  public static void validate(String value) {
    if (key(value).isEmpty() || value.codePointCount(0, value.length()) > 128)
      throw new IllegalArgumentException(
          "Glossary name or source reference must contain 1 to 128 code points");
  }
}
