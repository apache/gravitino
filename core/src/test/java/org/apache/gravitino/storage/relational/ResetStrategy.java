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

package org.apache.gravitino.storage.relational;

/**
 * What is rebuilt before each database-backed test, cheapest first. The first two share the test
 * class's database; {@link #FRESH_DATABASE} builds its own.
 */
public enum ResetStrategy {
  /** Rows are deleted from every non-empty table. {@code AUTO_INCREMENT} counters keep counting. */
  DELETE_ROWS,

  /**
   * Every table is truncated, which restarts its {@code AUTO_INCREMENT} counters. Slower than
   * {@link #DELETE_ROWS} because {@code TRUNCATE} recreates each table; use it only when a test
   * relies on the counters.
   */
  TRUNCATE_TABLES,

  /**
   * A new database (a new schema on PostgreSQL) and backend for the test, closed afterwards. Use it
   * when a test changes the schema, closes the backend itself, or leaves writers running after it
   * ends. Costs a full schema build per test.
   */
  FRESH_DATABASE
}
