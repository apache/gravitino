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

import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 * Carries the Iceberg constructor call for {@link SparkIcebergTable}. Iceberg publishes one Spark
 * module per Spark line and their {@code SparkTable} constructors differ, so each line has its own
 * copy of this class while the table itself stays shared.
 */
public abstract class SparkIcebergTableBase extends SparkTable {

  private final SparkTable sparkTable;

  /**
   * Creates the Iceberg table this wrapper stands in for.
   *
   * @param sparkTable the Iceberg table Spark loaded
   * @param sparkCatalog the Iceberg catalog that loaded it, unused on this Spark line
   */
  protected SparkIcebergTableBase(SparkTable sparkTable, SparkCatalog sparkCatalog) {
    // Iceberg's Spark 4.1 module publishes only SparkTable(Table). The earlier lines pass an
    // eager-refresh flag derived from the catalog's cacheEnabled field; 4.1 has no such parameter.
    super(sparkTable.table());
    this.sparkTable = sparkTable;
  }

  // The constructor above always builds the main branch, while the loaded table can be a branch
  // (see GravitinoIcebergCatalogSpark41#loadTable). Iceberg's ResolveBranch reads branch() to
  // decide whether the relation still has to be switched to the requested branch.

  @Override
  public String branch() {
    return sparkTable.branch();
  }

  @Override
  public Long snapshotId() {
    return sparkTable.snapshotId();
  }

  @Override
  public String version() {
    return sparkTable.version();
  }

  // The inherited write and delete methods ignore the branch the table was loaded for, so a table
  // loaded for a branch refuses them instead of writing elsewhere.

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    rejectWriteToBranch();
    return super.newWriteBuilder(info);
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    rejectWriteToBranch();
    return super.newRowLevelOperationBuilder(info);
  }

  @Override
  public boolean canDeleteWhere(Predicate[] predicates) {
    rejectWriteToBranch();
    return super.canDeleteWhere(predicates);
  }

  @Override
  public void deleteWhere(Predicate[] predicates) {
    rejectWriteToBranch();
    super.deleteWhere(predicates);
  }

  private void rejectWriteToBranch() {
    if (sparkTable.branch() != null) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot write to %s through a table loaded for branch %s",
              name(), sparkTable.branch()));
    }
  }
}
