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
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.CommandExecutionMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Wraps a Hive view stored in HMS with Gravitino view metadata. Overrides newScanBuilder to execute
 * the view's SQL (spark dialect preferred, hive as fallback) instead of using Kyuubi's file-based
 * scan, which returns empty results for VIRTUAL_VIEW entries.
 *
 * <p>Uses {@link LocalScan} to materialize view results on the driver. This is a known limitation:
 * the proper fix (expanding the view SQL into the query plan at analysis time) requires Spark's V2
 * catalog framework to route view operations to named catalogs, which Spark does not yet support —
 * {@code ResolveSessionCatalog} unconditionally throws for any non-session catalog view. Until
 * Spark adds that support, driver-side materialization via {@code executeCollect()} is the only
 * viable approach. <b>This implementation is suitable only for small/bounded views.</b>
 */
public class SparkHiveView extends HiveTable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHiveView.class);

  private final View gravitinoView;
  private final SparkTypeConverter sparkTypeConverter;
  private final String catalogName;
  private final String schemaName;

  /**
   * Creates a SparkHiveView that wraps a Gravitino view with Hive HMS backing.
   *
   * @param gravitinoView the Gravitino view metadata
   * @param catalogName the Spark catalog name the view was loaded from
   * @param schemaName the view's schema (database) name
   * @param hiveTable the backing Kyuubi HiveTable from HMS
   * @param hiveTableCatalog the Kyuubi HiveTableCatalog for HMS access
   * @param sparkTypeConverter converter for Gravitino-to-Spark type mapping
   */
  public SparkHiveView(
      View gravitinoView,
      String catalogName,
      String schemaName,
      HiveTable hiveTable,
      HiveTableCatalog hiveTableCatalog,
      SparkTypeConverter sparkTypeConverter) {
    super(SparkSession.active(), hiveTable.catalogTable(), hiveTableCatalog);
    this.gravitinoView = gravitinoView;
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.sparkTypeConverter = sparkTypeConverter;
  }

  @Override
  public String name() {
    return gravitinoView.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    org.apache.gravitino.rel.Column[] columns = gravitinoView.columns();
    if (columns.length == 0) {
      // View API allows an empty column array when the output schema is unknown;
      // fall back to the HMS-derived schema from the parent HiveTable.
      return super.schema();
    }
    List<StructField> fields =
        Arrays.stream(columns)
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
                  // Hive stores the view text with its table names qualified as the expanded text.
                  return catalogTable().viewText().getOrElse(() -> hiveSql);
                });
    return new ViewScanBuilder(spark, viewSql, schema(), relationQualifier(spark));
  }

  /**
   * Builds the qualifier for the namespace the view SQL resolves in: the catalog and namespace
   * Spark recorded when it created the view, or else the view's own catalog and schema. A recorded
   * session catalog means Spark created the view in this metastore through its session catalog, so
   * it maps to the catalog the view was loaded from.
   */
  @SuppressWarnings("deprecation") // JavaConverters is deprecated on Scala 2.13 but needed on 2.12
  private ViewRelationQualifier relationQualifier(SparkSession spark) {
    CatalogManager catalogManager = spark.sessionState().catalogManager();
    List<String> recorded = JavaConverters.seqAsJavaList(catalogTable().viewCatalogAndNamespace());
    String viewCatalog = catalogName;
    List<String> viewNamespace = new ArrayList<>();
    if (recorded.isEmpty()) {
      viewNamespace.add(schemaName);
    } else {
      if (!CatalogManager.SESSION_CATALOG_NAME().equals(recorded.get(0))) {
        viewCatalog = recorded.get(0);
      }
      viewNamespace.addAll(recorded.subList(1, recorded.size()));
    }
    return new ViewRelationQualifier(
        viewCatalog, viewNamespace, catalogManager::isCatalogRegistered);
  }

  /**
   * A ScanBuilder that executes the view's SQL via SparkSession using {@link LocalScan}. Spark
   * treats LocalScan results as driver-local data and never serializes rows to executors.
   */
  private static class ViewScanBuilder implements ScanBuilder, Scan, LocalScan {

    private final SparkSession spark;
    private final String viewSql;
    private final StructType schema;
    private final ViewRelationQualifier relationQualifier;

    ViewScanBuilder(
        SparkSession spark,
        String viewSql,
        StructType schema,
        ViewRelationQualifier relationQualifier) {
      this.spark = spark;
      this.viewSql = viewSql;
      this.schema = schema;
      this.relationQualifier = relationQualifier;
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
    public InternalRow[] rows() {
      // executeCollect() pulls all rows to driver memory — see class-level Javadoc for why.
      try {
        // Qualify the table names instead of switching the session's current namespace, so the
        // session and other queries running on it are not affected.
        LogicalPlan plan =
            relationQualifier.qualify(spark.sessionState().sqlParser().parsePlan(viewSql));
        return spark
            .sessionState()
            .executePlan(plan, CommandExecutionMode.ALL())
            .executedPlan()
            .executeCollect();
      } catch (ParseException | RuntimeException e) {
        throw new RuntimeException(
            String.format("Failed to execute view SQL [%s]: %s", viewSql, e.getMessage()), e);
      }
    }
  }
}
