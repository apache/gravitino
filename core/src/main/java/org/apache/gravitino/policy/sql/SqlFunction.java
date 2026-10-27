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
  /** Current user. */
  CURRENT_USER,
  /** Is group member. */
  IS_GROUP_MEMBER,
  /** User attribute. */
  USER_ATTRIBUTE,
  /** Is nan. */
  IS_NAN,
  /** Year. */
  YEAR,
  /** Month. */
  MONTH,
  /** Day. */
  DAY,
  /** Mask alphanum. */
  MASK_ALPHANUM,
  /** Show first 4. */
  SHOW_FIRST_4,
  /** Show last 4. */
  SHOW_LAST_4,
  /** Mask to fixed value. */
  MASK_TO_FIXED_VALUE,
  /** Replace with null. */
  REPLACE_WITH_NULL,
  /** Truncate to year. */
  TRUNCATE_TO_YEAR,
  /** Truncate to month. */
  TRUNCATE_TO_MONTH,
  /** Sha256 global. */
  SHA256_GLOBAL,
  /** Sha256 query local. */
  SHA256_QUERY_LOCAL;

  static SqlFunction fromName(String name) {
    try {
      return valueOf(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown restricted-sql-v1 function: " + name, e);
    }
  }
}
