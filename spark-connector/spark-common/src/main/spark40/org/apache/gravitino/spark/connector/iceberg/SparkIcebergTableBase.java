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

package org.apache.gravitino.spark.connector.iceberg;

import java.lang.reflect.Field;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;

/**
 * Carries the Iceberg constructor call for {@link SparkIcebergTable}. Iceberg publishes one Spark
 * module per Spark line and their {@code SparkTable} constructors differ, so each line has its own
 * copy of this class while the table itself stays shared.
 */
public abstract class SparkIcebergTableBase extends SparkTable {

  /**
   * Creates the Iceberg table this wrapper stands in for.
   *
   * @param sparkTable the Iceberg table Spark loaded
   * @param sparkCatalog the Iceberg catalog that loaded it, read for its cache setting
   */
  protected SparkIcebergTableBase(SparkTable sparkTable, SparkCatalog sparkCatalog) {
    super(sparkTable.table(), !isCacheEnabled(sparkCatalog));
  }

  private static boolean isCacheEnabled(SparkCatalog sparkCatalog) {
    try {
      Field cacheEnabled = sparkCatalog.getClass().getDeclaredField("cacheEnabled");
      cacheEnabled.setAccessible(true);
      return cacheEnabled.getBoolean(sparkCatalog);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to get cacheEnabled field from SparkCatalog", e);
    }
  }
}
