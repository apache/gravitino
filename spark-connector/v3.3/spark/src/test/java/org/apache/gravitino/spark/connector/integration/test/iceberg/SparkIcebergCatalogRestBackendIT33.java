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
import org.junit.jupiter.api.condition.DisabledIf;

// Spark 3.3 uses Iceberg 1.8.x; lakehouse-iceberg (1.11) runs in embedded MiniGravitino in the
// same JVM. Run REST-backend Iceberg IT in deploy mode only.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public class SparkIcebergCatalogRestBackendIT33 extends SparkIcebergCatalogRestBackendIT {
  @Override
  protected boolean supportsFunction() {
    // Spark 3.3 does not support function operations
    return false;
  }

  /**
   * Spark 3.3 uses Iceberg 1.8.x; its {@code SparkTable#metadataColumns()} exposes five columns,
   * not the seven row-lineage columns added in Iceberg 1.11.
   */
  @Override
  protected SparkMetadataColumnInfo[] getIcebergMetadataColumns() {
    return new SparkMetadataColumnInfo[] {
      new SparkMetadataColumnInfo("_spec_id", DataTypes.IntegerType, false),
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
