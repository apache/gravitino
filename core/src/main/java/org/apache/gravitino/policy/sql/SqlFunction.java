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
enum SqlFunction {
  CURRENT_USER,
  IS_GROUP_MEMBER,
  USER_ATTRIBUTE,
  IS_NAN,
  YEAR,
  MONTH,
  DAY,
  MASK_ALPHANUM,
  SHOW_FIRST_4,
  SHOW_LAST_4,
  MASK_TO_FIXED_VALUE,
  REPLACE_WITH_NULL,
  TRUNCATE_TO_YEAR,
  TRUNCATE_TO_MONTH,
  SHA256_GLOBAL,
  SHA256_QUERY_LOCAL;

  static SqlFunction fromName(String name) {
    try {
      return valueOf(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown restricted-sql-v1 function: " + name, e);
    }
  }
}
