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

package org.apache.gravitino.spark.connector.jdbc;

import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;

public class GravitinoJdbcCatalogSpark35 extends GravitinoJdbcCatalogSpark34 {

  @Override
  protected boolean supportsTimestampNtz() {
    // Spark 3.4 reads NTZ through java.sql.Timestamp and shifts it with the JVM time zone.
    // Spark 3.5 uses LocalDateTime, preserving the stored wall-clock value.
    return true;
  }

  @Override
  public Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
      throws NoSuchTableException {
    return loadTableForWriting(ident);
  }
}
