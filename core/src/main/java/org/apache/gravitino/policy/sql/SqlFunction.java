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
package org.apache.gravitino.policy.sql;

import java.util.Locale;

/** Allowlisted context, predicate, date-part and mask functions. */
public enum SqlFunction {
  /** Returns the trusted effective user's name. */
  CURRENT_USER,
  /** Tests the effective user's flat membership in a named group. */
  IS_GROUP_MEMBER,
  /** Tests whether a FLOAT or DOUBLE value is NaN. */
  IS_NAN,
  /** Extracts the calendar year from a DATE or TIMESTAMP. */
  YEAR,
  /** Extracts the month of the year from a DATE or TIMESTAMP. */
  MONTH,
  /** Extracts the day of the month from a DATE or TIMESTAMP. */
  DAY,
  /** Replaces string characters with the profile's digit and character masks. */
  MASK_ALPHANUM,
  /** Preserves the first four string code points and masks the remainder. */
  SHOW_FIRST_4,
  /** Preserves the last four string code points and masks the remainder. */
  SHOW_LAST_4,
  /** Replaces the input with the fixed mask value for its logical type. */
  MASK_TO_FIXED_VALUE,
  /** Replaces the input with a NULL of the same logical type. */
  REPLACE_WITH_NULL,
  /** Truncates a DATE or TIMESTAMP to the start of its year. */
  TRUNCATE_TO_YEAR,
  /** Truncates a DATE or TIMESTAMP to the start of its month. */
  TRUNCATE_TO_MONTH,
  /** Applies the type-preserving SHA-256 mask. */
  SHA256_GLOBAL,
  /** Applies the type-preserving SHA-256 mask with a salt shared within one query. */
  SHA256_QUERY_LOCAL;

  static SqlFunction fromName(String name) {
    try {
      return valueOf(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown restricted-sql-v1 function: " + name, e);
    }
  }
}
