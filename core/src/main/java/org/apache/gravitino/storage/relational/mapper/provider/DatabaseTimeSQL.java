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
package org.apache.gravitino.storage.relational.mapper.provider;

/**
 * SQL expressions that evaluate to the current time in epoch milliseconds, using the database
 * clock.
 *
 * <p>Prefer these over a timestamp computed in the JVM whenever a value is written to or compared
 * against a millisecond column: in a multi-node deployment every server has its own clock, while
 * the database provides a single clock all of them agree on.
 *
 * <p>The expressions live here, rather than in one SQL provider, so that every provider can reuse
 * them. Most providers still inline the MySQL flavour; they can be migrated to {@link #MYSQL}
 * separately, since changing them is unrelated to any single feature.
 */
public final class DatabaseTimeSQL {

  /**
   * MySQL flavour, also used by H2 in {@code MODE=MYSQL}. This is the expression that 30+ SQL
   * providers currently inline.
   */
  public static final String MYSQL =
      "((UNIX_TIMESTAMP() * 1000.0) + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000)";

  /** PostgreSQL flavour. */
  public static final String POSTGRESQL =
      "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";

  private DatabaseTimeSQL() {}
}
