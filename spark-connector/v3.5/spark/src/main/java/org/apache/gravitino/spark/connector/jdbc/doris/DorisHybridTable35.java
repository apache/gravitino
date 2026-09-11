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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark 3.5 table that selects governed Doris read and opt-in batch-write behavior. */
final class DorisHybridTable35 implements Table, SupportsRead, SupportsWrite {

  private final String name;
  private final StructType schema;
  private final Map<String, String> properties;
  private final Table nativeTable;
  private final Table jdbcTable;
  private final org.apache.gravitino.rel.Table logicalTable;
  private final DorisReadSchema35 readSchema;
  private final DorisWritePolicy35 writePolicy;

  DorisHybridTable35(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table nativeTable,
      Table jdbcTable,
      DorisReadSchema35 readSchema,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter transformConverter,
      SparkTypeConverter typeConverter) {
    GravitinoTableInfoHelper tableInfo =
        new GravitinoTableInfoHelper(
            false,
            identifier,
            gravitinoTable,
            propertiesConverter,
            transformConverter,
            typeConverter);
    this.name = tableInfo.name();
    this.properties = ImmutableMap.copyOf(tableInfo.properties());
    this.schema = readSchema.schema();
    this.nativeTable = nativeTable;
    this.jdbcTable = jdbcTable;
    this.logicalTable = gravitinoTable;
    this.readSchema = readSchema;
    this.writePolicy = DorisWritePolicy35.disabled();
    if (!(nativeTable instanceof SupportsRead) || !(jdbcTable instanceof SupportsRead)) {
      throw new IllegalArgumentException("Doris read delegates must implement SupportsRead");
    }
  }

  private DorisHybridTable35(DorisHybridTable35 source, DorisWritePolicy35 writePolicy) {
    this.name = source.name;
    this.schema = source.schema;
    this.properties = source.properties;
    this.nativeTable = source.nativeTable;
    this.jdbcTable = source.jdbcTable;
    this.logicalTable = source.logicalTable;
    this.readSchema = source.readSchema;
    this.writePolicy = writePolicy;
  }

  DorisHybridTable35 withGovernedWrite(DorisWritePolicy35 writePolicy) {
    if (!writePolicy.enabled()) {
      throw new IllegalArgumentException("Governed Doris write policy is disabled");
    }
    if (!(nativeTable instanceof SupportsWrite)) {
      throw new IllegalArgumentException("The physical Doris table does not support writes");
    }
    return new DorisHybridTable35(this, writePolicy);
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
    return DorisCapabilityPolicy35.from(writePolicy).tableCapabilities();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    DorisPropertiesConverter35.validateReadOptions(options);
    CaseInsensitiveStringMap governedOptions = new CaseInsensitiveStringMap(Collections.emptyMap());
    return new DorisHybridScanBuilder35(
        ((SupportsRead) nativeTable).newScanBuilder(governedOptions),
        ((SupportsRead) jdbcTable).newScanBuilder(governedOptions),
        readSchema.requiresSqlExecution(),
        readSchema.normalizedColumns());
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (!writePolicy.enabled() || !(nativeTable instanceof SupportsWrite)) {
      throw DorisCapabilityPolicy35.readOnly().reject("table writes");
    }
    if (info == null) {
      throw new IllegalArgumentException("Doris logical write information must not be null");
    }
    DorisPropertiesConverter35.validateWriteOptions(info.options());
    DorisWriteSchemaCompatibility35.Validator validator =
        DorisWriteSchemaCompatibility35.validate(logicalTable, readSchema, info.schema());
    return new GovernedDorisWriteBuilder35(
        ((SupportsWrite) nativeTable).newWriteBuilder(info), writePolicy, validator);
  }
}
