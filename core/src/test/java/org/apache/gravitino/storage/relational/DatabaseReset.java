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
 * What a shared database guarantees at the start of each test; see {@link DatabaseFixture#reset()}.
 */
public enum DatabaseReset {
  /** Every table is empty. {@code AUTO_INCREMENT} counters may keep counting from earlier tests. */
  DELETE_ROWS,

  /**
   * Every table is empty and its {@code AUTO_INCREMENT} counters restart. Slower, because each
   * {@code TRUNCATE} recreates the table, so use it only when a test relies on the counters.
   */
  TRUNCATE_TABLES
}
