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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Coordinates the official Doris detail reader with Spark's JDBC query planner. */
final class DorisHybridScanBuilder35
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownV2Filters,
        SupportsPushDownLimit,
        SupportsPushDownAggregates,
        SupportsPushDownTopN,
        SupportsPushDownOffset {

  private final ScanBuilder nativeBuilder;
  private final ScanBuilder jdbcBuilder;
  private final Set<String> timestampColumns = new HashSet<>();
  private boolean useJdbc;
  private Integer pushedUpperBound;
  private Predicate[] pushedPredicates = new Predicate[0];

  DorisHybridScanBuilder35(ScanBuilder nativeBuilder, ScanBuilder jdbcBuilder, StructType schema) {
    if (!(nativeBuilder instanceof SupportsPushDownRequiredColumns)
        || !(nativeBuilder instanceof SupportsPushDownV2Filters)
        || !(jdbcBuilder instanceof SupportsPushDownRequiredColumns)
        || !(jdbcBuilder instanceof SupportsPushDownV2Filters)
        || !(jdbcBuilder instanceof SupportsPushDownLimit)
        || !(jdbcBuilder instanceof SupportsPushDownAggregates)
        || !(jdbcBuilder instanceof SupportsPushDownTopN)
        || !(jdbcBuilder instanceof SupportsPushDownOffset)) {
      throw new IllegalStateException("Doris scan delegates do not support the required APIs");
    }
    this.nativeBuilder = nativeBuilder;
    this.jdbcBuilder = jdbcBuilder;
    for (StructField field : schema.fields()) {
      if (DataTypes.TimestampType.equals(field.dataType())
          || DataTypes.TimestampNTZType.equals(field.dataType())) {
        timestampColumns.add(field.name().toLowerCase(Locale.ROOT));
      }
    }
  }

  @Override
  public Scan build() {
    return (useJdbc ? jdbcBuilder : nativeBuilder).build();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    ((SupportsPushDownRequiredColumns) jdbcBuilder).pruneColumns(requiredSchema);
    // Native timestamp decoding uses the executor's default time zone, whereas JDBC may use a
    // configured connection time zone. Spark includes columns needed for residual filters here.
    for (String field : requiredSchema.fieldNames()) {
      if (timestampColumns.contains(field.toLowerCase(Locale.ROOT))) {
        useJdbc = true;
        break;
      }
    }
    // Pushed JDBC aggregates can change the output schema to synthetic aggregate columns, which
    // are not columns of the physical Doris table. Spark prunes after selecting the SQL delegate.
    if (!useJdbc) {
      ((SupportsPushDownRequiredColumns) nativeBuilder).pruneColumns(requiredSchema);
    }
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    List<Predicate> jdbcCandidates = new ArrayList<>();
    for (Predicate predicate : predicates) {
      if (!referencesTimestamp(predicate)) {
        jdbcCandidates.add(predicate);
      }
    }
    Predicate[] jdbcResidual =
        ((SupportsPushDownV2Filters) jdbcBuilder)
            .pushPredicates(jdbcCandidates.toArray(new Predicate[0]));
    List<Predicate> candidates = new ArrayList<>();
    for (Predicate predicate : predicates) {
      if (!Arrays.asList(jdbcResidual).contains(predicate) && !referencesTimestamp(predicate)) {
        candidates.add(predicate);
      }
    }
    Predicate[] nativeResidual =
        ((SupportsPushDownV2Filters) nativeBuilder)
            .pushPredicates(candidates.toArray(new Predicate[0]));
    List<Predicate> residual = new ArrayList<>();
    List<Predicate> pushed = new ArrayList<>();
    for (Predicate predicate : predicates) {
      if (referencesTimestamp(predicate)
          || Arrays.asList(nativeResidual).contains(predicate)
          || Arrays.asList(jdbcResidual).contains(predicate)) {
        residual.add(predicate);
      } else {
        pushed.add(predicate);
      }
    }
    pushedPredicates = pushed.toArray(new Predicate[0]);
    return residual.toArray(new Predicate[0]);
  }

  @Override
  public Predicate[] pushedPredicates() {
    return Arrays.copyOf(pushedPredicates, pushedPredicates.length);
  }

  @Override
  public boolean pushLimit(int limit) {
    boolean accepted = ((SupportsPushDownLimit) jdbcBuilder).pushLimit(limit);
    if (accepted) {
      useJdbc = true;
      pushedUpperBound = limit;
    }
    return accepted;
  }

  @Override
  public boolean supportCompletePushDown(Aggregation aggregation) {
    return ((SupportsPushDownAggregates) jdbcBuilder).supportCompletePushDown(aggregation);
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    boolean accepted = ((SupportsPushDownAggregates) jdbcBuilder).pushAggregation(aggregation);
    useJdbc |= accepted;
    return accepted;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    boolean accepted = ((SupportsPushDownTopN) jdbcBuilder).pushTopN(orders, limit);
    if (accepted) {
      useJdbc = true;
      pushedUpperBound = limit;
    }
    return accepted;
  }

  @Override
  public boolean isPartiallyPushed() {
    return ((SupportsPushDownTopN) jdbcBuilder).isPartiallyPushed();
  }

  @Override
  public boolean pushOffset(int offset) {
    if (pushedUpperBound != null && offset >= pushedUpperBound) {
      return false;
    }
    boolean accepted = ((SupportsPushDownOffset) jdbcBuilder).pushOffset(offset);
    if (accepted) {
      useJdbc = true;
      if (pushedUpperBound != null) {
        pushedUpperBound -= offset;
      }
    }
    return accepted;
  }

  private boolean referencesTimestamp(Predicate predicate) {
    if ("IS_NULL".equals(predicate.name()) || "IS_NOT_NULL".equals(predicate.name())) {
      return false;
    }
    // Doris 26.1.0 renders timestamps through java.sql.Timestamp.toString(), which depends on
    // the JVM default time zone and may differ from Spark's session time zone.
    for (NamedReference reference : predicate.references()) {
      String[] fields = reference.fieldNames();
      if (fields.length > 0 && timestampColumns.contains(fields[0].toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }
}
