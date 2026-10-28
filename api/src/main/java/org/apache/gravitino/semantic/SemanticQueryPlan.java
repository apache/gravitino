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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * The SQL produced for a {@link SemanticQuery}, together with the dialect and planner that produced
 * it.
 *
 * <p>The server compiles a plan without executing it; the SQL is returned to the caller and is not
 * cached.
 */
@Evolving
public final class SemanticQueryPlan {

  private final String sql;
  private final String dialect;
  private final String planner;
  private final List<String> warnings;

  private SemanticQueryPlan(String sql, String dialect, String planner, List<String> warnings) {
    Preconditions.checkArgument(StringUtils.isNotBlank(sql), "sql must not be null or blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dialect), "dialect must not be null or blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(planner), "planner must not be null or blank");
    Preconditions.checkArgument(warnings != null, "warnings must not be null");
    for (int index = 0; index < warnings.size(); index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(warnings.get(index)),
          "warnings[%s] must not be null or blank",
          index);
    }
    this.sql = sql;
    this.dialect = dialect;
    this.planner = planner;
    this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
  }

  /**
   * Creates a query plan.
   *
   * @param sql The generated SQL.
   * @param dialect The dialect the SQL targets.
   * @param planner The name of the planner that generated the SQL.
   * @param warnings Non-fatal warnings produced while planning; an empty list for none.
   * @return The immutable plan.
   * @throws IllegalArgumentException If a required value is null or blank.
   */
  public static SemanticQueryPlan of(
      String sql, String dialect, String planner, List<String> warnings) {
    return new SemanticQueryPlan(sql, dialect, planner, warnings);
  }

  /**
   * Returns the generated SQL.
   *
   * @return The SQL text.
   */
  public String sql() {
    return sql;
  }

  /**
   * Returns the dialect the SQL targets.
   *
   * @return The dialect identifier.
   */
  public String dialect() {
    return dialect;
  }

  /**
   * Returns the name of the planner that generated the SQL.
   *
   * @return The planner name.
   */
  public String planner() {
    return planner;
  }

  /**
   * Returns non-fatal warnings produced while planning.
   *
   * @return An unmodifiable list of warnings; empty if there are none.
   */
  public List<String> warnings() {
    return warnings;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemanticQueryPlan)) {
      return false;
    }
    SemanticQueryPlan that = (SemanticQueryPlan) other;
    return sql.equals(that.sql)
        && dialect.equals(that.dialect)
        && planner.equals(that.planner)
        && warnings.equals(that.warnings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, dialect, planner, warnings);
  }

  @Override
  public String toString() {
    return "SemanticQueryPlan{"
        + "sql='"
        + sql
        + '\''
        + ", dialect='"
        + dialect
        + '\''
        + ", planner='"
        + planner
        + '\''
        + ", warnings="
        + warnings
        + '}';
  }
}
