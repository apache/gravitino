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
package org.apache.gravitino.rel.metric;

import org.apache.gravitino.annotation.Unstable;

/**
 * Predefined expression dialect identifiers used by {@link DialectExpression#dialect()}.
 *
 * <p>These constants match the dialects defined by the pinned Apache Ossie schema. The API uses
 * strings so dialect identifiers remain extensible; the active Metric View profile determines which
 * values it supports.
 */
@Unstable
public final class MetricDialects {

  /** ANSI SQL. */
  public static final String ANSI_SQL = "ANSI_SQL";

  /** Snowflake SQL. */
  public static final String SNOWFLAKE = "SNOWFLAKE";

  /** Multidimensional Expressions. */
  public static final String MDX = "MDX";

  /** Tableau expression language. */
  public static final String TABLEAU = "TABLEAU";

  /** Databricks SQL. */
  public static final String DATABRICKS = "DATABRICKS";

  /** MicroStrategy Analytical Engine language. */
  public static final String MAQL = "MAQL";

  /** GoogleSQL for BigQuery. */
  public static final String BIGQUERY = "BIGQUERY";

  private MetricDialects() {}
}
