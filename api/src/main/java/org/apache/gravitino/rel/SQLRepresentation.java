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
package org.apache.gravitino.rel;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.gravitino.annotation.Unstable;

/**
 * A SQL-based {@link Representation} of a view. Each {@code SQLRepresentation} carries the SQL text
 * together with the {@link #dialect() dialect} it is expressed in.
 *
 * <p>Default catalog and schema used to resolve unqualified identifiers referenced by the SQL are
 * defined at the {@link View} level and shared across all representations of the view.
 */
@Unstable
public final class SQLRepresentation implements Representation {

  private final String dialect;
  private final String sql;

  private SQLRepresentation(String dialect, String sql) {
    this.dialect = dialect;
    this.sql = sql;
  }

  /**
   * Creates a new builder for {@link SQLRepresentation}.
   *
   * @return A new {@link Builder} instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String type() {
    return Representation.TYPE_SQL;
  }

  /**
   * Returns the SQL dialect of this representation, e.g. {@code "trino"} or {@code "spark"}. See
   * {@link Dialects} for well-known values.
   *
   * @return The dialect identifier.
   */
  public String dialect() {
    return dialect;
  }

  /**
   * Returns the SQL text of this representation.
   *
   * @return The SQL text.
   */
  public String sql() {
    return sql;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SQLRepresentation that = (SQLRepresentation) o;
    return dialect.equals(that.dialect) && sql.equals(that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dialect, sql);
  }

  @Override
  public String toString() {
    return "SQLRepresentation{" + "dialect='" + dialect + '\'' + ", sql='" + sql + '\'' + '}';
  }

  /** A builder for {@link SQLRepresentation}. */
  public static final class Builder {

    private String dialect;
    private String sql;

    private Builder() {}

    /**
     * Sets the SQL dialect.
     *
     * @param dialect The dialect identifier; must be non-null and non-empty.
     * @return This builder.
     */
    public Builder withDialect(String dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * Sets the SQL text.
     *
     * @param sql The SQL text; must be non-null and non-empty.
     * @return This builder.
     */
    public Builder withSql(String sql) {
      this.sql = sql;
      return this;
    }

    /**
     * Builds a new {@link SQLRepresentation}.
     *
     * @return The constructed {@link SQLRepresentation}.
     * @throws IllegalArgumentException If {@code dialect} or {@code sql} is null or empty.
     */
    public SQLRepresentation build() {
      Preconditions.checkArgument(
          dialect != null && !dialect.isEmpty(), "dialect must not be null or empty");
      Preconditions.checkArgument(sql != null && !sql.isEmpty(), "sql must not be null or empty");
      return new SQLRepresentation(dialect, sql);
    }
  }
}
