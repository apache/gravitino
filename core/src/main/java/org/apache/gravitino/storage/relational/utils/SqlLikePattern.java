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

package org.apache.gravitino.storage.relational.utils;

/** Literal matching in parameterized SQL LIKE patterns on the relational backends. */
public final class SqlLikePattern {
  private static final String ESCAPE = "!";

  /** SQL clause paired with {@link #escapeLiteral(String)}. */
  public static final String ESCAPE_CLAUSE = "ESCAPE '" + ESCAPE + "'";

  private SqlLikePattern() {}

  /**
   * Escapes the escape character and both wildcards in a literal part of a LIKE pattern.
   *
   * @param value literal text to match
   * @return the escaped text for use in a bound pattern
   */
  public static String escapeLiteral(String value) {
    return value
        .replace(ESCAPE, ESCAPE + ESCAPE)
        .replace("%", ESCAPE + "%")
        .replace("_", ESCAPE + "_");
  }
}
