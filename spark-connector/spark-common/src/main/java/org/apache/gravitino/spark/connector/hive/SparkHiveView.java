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

package org.apache.gravitino.spark.connector.hive;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a Hive view stored in HMS with Gravitino view metadata. Overrides newScanBuilder to execute
 * the view's SQL (spark dialect preferred, hive as fallback) instead of using Kyuubi's file-based
 * scan, which returns empty results for VIRTUAL_VIEW entries.
 *
 * <p><b>Note:</b> View results are materialized on the driver via {@code executeCollect()}. This is
 * intentional to avoid executor-side SparkSession access, but limits scalability to views whose
 * results fit in driver memory.
 */
public class SparkHiveView extends HiveTable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHiveView.class);

  private final View gravitinoView;
  private final SparkTypeConverter sparkTypeConverter;

  /**
   * Creates a SparkHiveView that wraps a Gravitino view with Hive HMS backing.
   *
   * @param gravitinoView the Gravitino view metadata
   * @param hiveTable the backing Kyuubi HiveTable from HMS
   * @param hiveTableCatalog the Kyuubi HiveTableCatalog for HMS access
   * @param sparkTypeConverter converter for Gravitino-to-Spark type mapping
   */
  public SparkHiveView(
      View gravitinoView,
      HiveTable hiveTable,
      HiveTableCatalog hiveTableCatalog,
      SparkTypeConverter sparkTypeConverter) {
    super(SparkSession.active(), hiveTable.catalogTable(), hiveTableCatalog);
    this.gravitinoView = gravitinoView;
    this.sparkTypeConverter = sparkTypeConverter;
  }

  @Override
  public String name() {
    return gravitinoView.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    List<StructField> fields =
        Arrays.stream(gravitinoView.columns())
            .map(
                column -> {
                  String comment = column.comment();
                  Metadata metadata =
                      comment != null
                          ? new MetadataBuilder()
                              .putString(ConnectorConstants.COMMENT, comment)
                              .build()
                          : Metadata.empty();
                  return StructField.apply(
                      column.name(),
                      sparkTypeConverter.toSparkType(column.dataType()),
                      column.nullable(),
                      metadata);
                })
            .collect(Collectors.toList());
    return DataTypes.createStructType(fields);
  }

  @Override
  public Map<String, String> properties() {
    return gravitinoView.properties();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    SparkSession spark = SparkSession.active();
    Preconditions.checkState(
        spark != null && !spark.sparkContext().isStopped(),
        "No active SparkSession available to execute view '%s'",
        gravitinoView.name());
    String viewSql =
        gravitinoView
            .sqlFor(Dialects.SPARK)
            .map(SQLRepresentation::sql)
            .orElseGet(
                () -> {
                  String hiveSql =
                      gravitinoView
                          .sqlFor(Dialects.HIVE)
                          .map(SQLRepresentation::sql)
                          .orElseThrow(
                              () ->
                                  new UnsupportedOperationException(
                                      "No SQL representation for view: " + gravitinoView.name()));
                  LOG.warn(
                      "View '{}' has no Spark SQL representation; falling back to Hive SQL. "
                          + "Results may differ if the SQL uses Hive-specific syntax.",
                      gravitinoView.name());
                  return hiveSql;
                });
    return new ViewScanBuilder(spark, viewSql, schema());
  }

  /**
   * A ScanBuilder that executes the view's SQL via SparkSession and returns the resulting rows.
   * This is used for views in V2 catalogs where Kyuubi's file-based scan cannot expand the view.
   */
  private static class ViewScanBuilder implements ScanBuilder, Scan, Batch, PartitionReaderFactory {

    private final SparkSession spark;
    private final String viewSql;
    private final StructType schema;

    ViewScanBuilder(SparkSession spark, String viewSql, StructType schema) {
      this.spark = spark;
      this.viewSql = viewSql;
      this.schema = schema;
    }

    @Override
    public Scan build() {
      return this;
    }

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public Batch toBatch() {
      return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      try {
        InternalRow[] rows = spark.sql(viewSql).queryExecution().executedPlan().executeCollect();
        return new InputPartition[] {new ViewPartition(rows)};
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to execute view SQL [%s]: %s", viewSql, e.getMessage()), e);
      }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return this;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      InternalRow[] rows = ((ViewPartition) partition).rows;
      Iterator<InternalRow> iter = Arrays.asList(rows).iterator();
      return new PartitionReader<InternalRow>() {
        private InternalRow current;

        @Override
        public boolean next() {
          if (iter.hasNext()) {
            current = iter.next();
            return true;
          }
          return false;
        }

        @Override
        public InternalRow get() {
          return current;
        }

        @Override
        public void close() {}
      };
    }

    private static class ViewPartition implements InputPartition {
      final InternalRow[] rows;

      ViewPartition(InternalRow[] rows) {
        this.rows = rows;
      }
    }
  }
}
