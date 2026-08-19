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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Read-only Spark 3.5 table that selects between Doris native and JDBC lanes. */
final class DorisHybridTable35 implements Table, SupportsRead {

  private final String name;
  private final StructType schema;
  private final Map<String, String> properties;
  private final Table nativeTable;
  private final Table jdbcTable;
  private final DorisReadSchema35 readSchema;

  DorisHybridTable35(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table nativeTable,
      Table jdbcTable,
      DorisReadSchema35 readSchema,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter transformConverter,
      SparkTypeConverter typeConverter) {
    this.name =
        new GravitinoTableInfoHelper(
                false,
                identifier,
                gravitinoTable,
                propertiesConverter,
                transformConverter,
                typeConverter)
            .name();
    this.properties =
        ImmutableMap.copyOf(
            new GravitinoTableInfoHelper(
                    false,
                    identifier,
                    gravitinoTable,
                    propertiesConverter,
                    transformConverter,
                    typeConverter)
                .properties());
    this.schema = readSchema.schema();
    this.nativeTable = nativeTable;
    this.jdbcTable = jdbcTable;
    this.readSchema = readSchema;
    if (!(nativeTable instanceof SupportsRead) || !(jdbcTable instanceof SupportsRead)) {
      throw new IllegalArgumentException("Doris read delegates must implement SupportsRead");
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return schema;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DorisHybridScanBuilder35(
        ((SupportsRead) nativeTable).newScanBuilder(options),
        ((SupportsRead) jdbcTable).newScanBuilder(options),
        readSchema.requiresSqlExecution(),
        readSchema.normalizedColumns());
  }
}
