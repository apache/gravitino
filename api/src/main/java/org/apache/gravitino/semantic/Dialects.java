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
package org.apache.gravitino.semantic;

import org.apache.gravitino.annotation.Evolving;

/**
 * Well-known dialect identifiers for Semantic Model expressions.
 *
 * <p>These constants match the dialects defined by the pinned Apache Ossie schema. Applications may
 * use other non-empty identifiers; Gravitino preserves them without implicit conversion or fallback
 * so consumers can select the dialects they support.
 */
@Evolving
public final class Dialects {

  /** ANSI SQL. */
  public static final String ANSI_SQL = "ANSI_SQL";

  /** Snowflake SQL. */
  public static final String SNOWFLAKE = "SNOWFLAKE";

  /** Multidimensional Expressions (MDX). */
  public static final String MDX = "MDX";

  /** Tableau expression syntax. */
  public static final String TABLEAU = "TABLEAU";

  /** Databricks SQL. */
  public static final String DATABRICKS = "DATABRICKS";

  /** Multi-Dimensional Analytical Query Language (MAQL). */
  public static final String MAQL = "MAQL";

  /** Google BigQuery SQL. */
  public static final String BIGQUERY = "BIGQUERY";

  private Dialects() {}
}
