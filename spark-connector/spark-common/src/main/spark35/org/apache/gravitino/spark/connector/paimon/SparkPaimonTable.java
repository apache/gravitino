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

package org.apache.gravitino.spark.connector.paimon;

import java.util.Map;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.paimon.spark.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkPaimonTable extends SparkTable {

  private GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private org.apache.spark.sql.connector.catalog.Table sparkTable;

  public SparkPaimonTable(
      Identifier identifier,
      Table gravitinoTable,
      SparkTable sparkTable,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    super(sparkTable.getTable());
    this.gravitinoTableInfoHelper =
        new GravitinoTableInfoHelper(
            true,
            identifier,
            gravitinoTable,
            propertiesConverter,
            sparkTransformConverter,
            sparkTypeConverter);
    this.sparkTable = sparkTable;
  }

  @Override
  public String name() {
    return gravitinoTableInfoHelper.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return gravitinoTableInfoHelper.schema();
  }

  @Override
  public Map<String, String> properties() {
    return gravitinoTableInfoHelper.properties();
  }

  @Override
  public Transform[] partitioning() {
    return gravitinoTableInfoHelper.partitioning();
  }

  /**
   * If using SparkPaimonTable not SparkTable, we must extract snapshotId or branchName using the
   * Paimon specific logic. It's hard to maintenance.
   */
  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return ((SparkTable) sparkTable).newScanBuilder(options);
  }
}
