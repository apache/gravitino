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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * Syntactic checks for Iceberg {@code rewrite_data_files} {@code sort_order} strings.
 *
 * <p>Does not resolve column names against a table schema; missing columns are skipped at job
 * runtime. Accepts ordinary sort fields and {@code zorder(...)}.
 */
final class IcebergSortOrderSyntax {

  // Group the alternation so embedding IDENT into larger patterns does not change precedence.
  private static final String IDENT = "(?:[A-Za-z_][A-Za-z0-9_]*|`(?:[^`]+)`)";
  private static final Pattern ZORDER =
      Pattern.compile("(?i)^zorder\\s*\\(\\s*" + IDENT + "(?:\\s*,\\s*" + IDENT + ")*\\s*\\)$");
  private static final Pattern SORT_FIELD =
      Pattern.compile(
          "(?i)^\\s*"
              + IDENT
              + "(?:\\s*\\.\\s*"
              + IDENT
              + ")*"
              + "(?:\\s+(ASC|DESC))?"
              + "(?:\\s+NULLS\\s+(FIRST|LAST))?\\s*$");

  private IcebergSortOrderSyntax() {}

  /**
   * @param sortOrder non-blank sort order string
   * @throws IllegalArgumentException if the string is not a recognizable Iceberg sort order
   */
  static void validate(String sortOrder) {
    Preconditions.checkArgument(StringUtils.isNotBlank(sortOrder), "sortOrder must not be blank");
    String trimmed = sortOrder.trim();
    if (ZORDER.matcher(trimmed).matches()) {
      return;
    }
    String[] parts = trimmed.split(",");
    Preconditions.checkArgument(parts.length > 0, "sortOrder must not be empty");
    for (String part : parts) {
      Matcher field = SORT_FIELD.matcher(part);
      Preconditions.checkArgument(
          field.matches(),
          "sortOrder is not a valid Iceberg sort order (expected fields like 'id DESC NULLS LAST'"
              + " or 'zorder(c1,c2)'): '%s'",
          sortOrder);
    }
  }
}
