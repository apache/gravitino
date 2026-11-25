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

package org.apache.gravitino.spark.connector.jdbc.doris;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Read-only view of a validated Spark JDBC table for the specialized Doris path. */
final class DorisReadOnlyTable35 implements Table, SupportsRead {

  private final SparkJdbcTable jdbcTable;
  @Nullable private final Supplier<Table> nativeTable;
  private final Runnable validateNativeOptions;

  DorisReadOnlyTable35(SparkJdbcTable jdbcTable) {
    this(jdbcTable, null, () -> {});
  }

  DorisReadOnlyTable35(
      SparkJdbcTable jdbcTable,
      @Nullable Supplier<Table> nativeTable,
      Runnable validateNativeOptions) {
    this.jdbcTable = jdbcTable;
    this.nativeTable = nativeTable;
    this.validateNativeOptions = validateNativeOptions;
  }

  @Override
  public String name() {
    return jdbcTable.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return jdbcTable.schema();
  }

  @Override
  public Map<String, String> properties() {
    return jdbcTable.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (nativeTable == null) {
      DorisPropertiesConverter35.validateReadOptions(options);
      return jdbcTable.newScanBuilder(options);
    }
    DorisPropertiesConverter35.validateNativeReadOptions(options);
    // Creating the JDBC builder also checks the vended credential and JDBC connection snapshot.
    ScanBuilder jdbcBuilder = jdbcTable.newScanBuilder(options);
    validateNativeOptions.run();
    Table delegate = nativeTable.get();
    if (delegate == null) {
      return jdbcBuilder;
    }
    return new DorisHybridScanBuilder35(
        ((SupportsRead) delegate)
            .newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap())),
        jdbcBuilder,
        jdbcTable.schema());
  }
}
