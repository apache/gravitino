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

package org.apache.gravitino.spark.connector.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;

/**
 * A JDBC scan builder that keeps expressions in Spark when the source could evaluate them with
 * different semantics. Other predicates, aggregates, Top-N, limits, and offsets are pushed as
 * usual.
 */
public class SparkJdbcScanBuilder extends JDBCScanBuilder {

  private final transient SparkJdbcPushDownPolicy policy;

  SparkJdbcScanBuilder(JDBCScanBuilder delegate, SparkJdbcPushDownPolicy policy) {
    super(delegate.session(), delegate.schema(), delegate.jdbcOptions());
    this.policy = policy;
  }

  /**
   * Pushes only predicates whose results cannot change at the source.
   *
   * @param predicates the predicates Spark offers to the source
   * @return the predicates Spark must still evaluate after the scan
   */
  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    List<Predicate> kept = new ArrayList<>();
    List<Predicate> candidates = new ArrayList<>();
    for (Predicate predicate : predicates) {
      (policy.canPush(predicate) ? candidates : kept).add(predicate);
    }
    kept.addAll(Arrays.asList(super.pushPredicates(candidates.toArray(new Predicate[0]))));
    return kept.toArray(new Predicate[0]);
  }

  /**
   * Checks whether an aggregation can be fully evaluated by the source.
   *
   * @param aggregation the aggregation Spark offers to the source
   * @return false if any part of the aggregation must stay in Spark
   */
  @Override
  public boolean supportCompletePushDown(Aggregation aggregation) {
    return canPush(aggregation) && super.supportCompletePushDown(aggregation);
  }

  /**
   * Pushes an aggregation only when every function and group-by expression is safe.
   *
   * @param aggregation the aggregation Spark offers to the source
   * @return true if the source evaluates the aggregation
   */
  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    return canPush(aggregation) && super.pushAggregation(aggregation);
  }

  /**
   * Pushes Top-N only when every sort expression is safe.
   *
   * @param orders the sort orders
   * @param limit the number of rows to return
   * @return true if the source evaluates the Top-N
   */
  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    for (SortOrder order : orders) {
      if (!policy.canPush(order.expression())) {
        return false;
      }
    }
    return super.pushTopN(orders, limit);
  }

  private boolean canPush(Aggregation aggregation) {
    for (Expression expression : aggregation.aggregateExpressions()) {
      if (!policy.canPushAggregate(expression)) {
        return false;
      }
    }
    for (Expression expression : aggregation.groupByExpressions()) {
      if (!policy.canPushAggregate(expression)) {
        return false;
      }
    }
    return true;
  }
}
