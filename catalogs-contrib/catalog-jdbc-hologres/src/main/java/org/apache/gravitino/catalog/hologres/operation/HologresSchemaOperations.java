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
package org.apache.gravitino.catalog.hologres.operation;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;

/**
 * Schema (Database) operations for Hologres.
 *
 * <p>Hologres uses the PostgreSQL schema concept where Database in PostgreSQL corresponds to
 * Catalog in JDBC, and Schema in PostgreSQL corresponds to Schema in JDBC.
 *
 * <p>TODO: Full implementation will be added in a follow-up PR.
 */
public class HologresSchemaOperations extends JdbcDatabaseOperations {

  @Override
  protected boolean supportSchemaComment() {
    return true;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of(
        // PostgreSQL system schemas
        "pg_toast",
        "pg_catalog",
        "information_schema",
        // Hologres internal schemas
        "hologres",
        "hg_internal",
        "hg_recyclebin",
        "hologres_object_table",
        "hologres_sample",
        "hologres_streaming_mv",
        "hologres_statistic");
  }
}
