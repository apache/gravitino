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

import org.apache.spark.sql.connector.read.SupportsPushDownJoin;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;

/**
 * Carries the {@code JDBCScanBuilder} constructor call for {@link SparkJdbcScanBuilder}. Spark 4.1
 * added a fourth constructor parameter, so each Spark line has its own copy of this class while the
 * scan builder itself stays shared. Spark 4.1 also added join pushdown, which this copy turns off
 * when the builder is the left side of the join, the side Spark asks.
 */
public abstract class SparkJdbcScanBuilderBase extends JDBCScanBuilder {

  /**
   * Creates a scan builder for the same table and options as {@code delegate}.
   *
   * @param delegate the scan builder Spark created
   */
  protected SparkJdbcScanBuilderBase(JDBCScanBuilder delegate) {
    // Spark 4.1 added the additionalMetrics parameter; forward what the delegate reports.
    super(
        delegate.session(),
        delegate.schema(),
        delegate.jdbcOptions(),
        delegate.additionalMetrics());
  }

  /**
   * Keeps joins with this builder on the left in Spark. Spark 4.1 can push a join of two tables of
   * the same JDBC source. It compiles the join condition without {@link SparkJdbcPushDownPolicy},
   * and the left table's policy, which knows only that table's columns, then decides on aggregates
   * and Top-N over both tables.
   *
   * @param other the scan builder of the other side of the join
   * @return false
   */
  @Override
  public boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other) {
    return false;
  }
}
