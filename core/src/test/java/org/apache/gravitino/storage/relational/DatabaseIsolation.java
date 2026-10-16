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

/** What each database-backed test gets fresh, from the cheapest level to the most isolated. */
public enum DatabaseIsolation {
  /**
   * One backend and database per test class and backend type; {@link TestJDBCBackend} truncates
   * every table before each test.
   */
  RESET_TABLES,

  /**
   * A new database (a new schema on PostgreSQL) and backend for each test, closed afterwards. Use
   * it when a test changes the schema, closes the backend itself, or leaves writers running after
   * it ends.
   */
  FRESH_DATABASE
}
