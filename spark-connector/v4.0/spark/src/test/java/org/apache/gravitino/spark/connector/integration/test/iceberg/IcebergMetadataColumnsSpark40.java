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
package org.apache.gravitino.spark.connector.integration.test.iceberg;

import org.apache.gravitino.spark.connector.integration.test.util.SparkMetadataColumnInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * The metadata columns Iceberg reports from its Spark 4.0 module, which differ from its 3.5 one.
 *
 * <p>The row-lineage columns ({@code _row_id}, {@code _last_updated_sequence_number}) only show up
 * for tables that support row lineage, meaning format version 3 and above, while the 3.5 module
 * reports them unconditionally; these ITs create format version 2 tables, Iceberg's default. {@code
 * _spec_id} is also nullable here and non-nullable on 3.5.
 *
 * <p>Every Iceberg IT on this line needs the same answer, and the two backends inherit from
 * different shared bases, so it lives here rather than being overridden twice.
 */
final class IcebergMetadataColumnsSpark40 {

  private IcebergMetadataColumnsSpark40() {}

  /** Returns a fresh array each call: the shared ITs mutate entries of it in place. */
  static SparkMetadataColumnInfo[] newMetadataColumns() {
    return new SparkMetadataColumnInfo[] {
      new SparkMetadataColumnInfo("_spec_id", DataTypes.IntegerType, true),
      new SparkMetadataColumnInfo(
          "_partition",
          DataTypes.createStructType(
              new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)}),
          true),
      new SparkMetadataColumnInfo("_file", DataTypes.StringType, false),
      new SparkMetadataColumnInfo("_pos", DataTypes.LongType, false),
      new SparkMetadataColumnInfo("_deleted", DataTypes.BooleanType, false)
    };
  }
}
