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
package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;

/**
 * A DTO mirroring {@link org.apache.gravitino.rel.SQLRepresentation}. Represents a SQL-based view
 * definition for a particular dialect.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SQLRepresentationDTO extends RepresentationDTO {

  @JsonProperty("type")
  private final String type = Representation.TYPE_SQL;

  @JsonProperty("dialect")
  private String dialect;

  @JsonProperty("sql")
  private String sql;

  private SQLRepresentationDTO() {
    super();
  }

  private SQLRepresentationDTO(String dialect, String sql) {
    this.dialect = dialect;
    this.sql = sql;
  }

  /**
   * Creates a new {@link Builder}.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a new {@link SQLRepresentationDTO} from a {@link SQLRepresentation} domain object.
   *
   * @param sqlRepresentation The SQL representation domain object.
   * @return The SQL representation DTO.
   */
  public static SQLRepresentationDTO fromSQLRepresentation(SQLRepresentation sqlRepresentation) {
    return builder()
        .withDialect(sqlRepresentation.dialect())
        .withSql(sqlRepresentation.sql())
        .build();
  }

  @Override
  public String type() {
    return type;
  }

  /**
   * Returns the SQL dialect of this representation.
   *
   * @return The dialect identifier.
   */
  public String dialect() {
    return dialect;
  }

  /**
   * Returns the SQL dialect of this representation.
   *
   * @return The dialect identifier.
   */
  public String getDialect() {
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

  /**
   * Returns the SQL text of this representation.
   *
   * @return The SQL text.
   */
  public String getSql() {
    return sql;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dialect), "\"dialect\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sql), "\"sql\" field is required and cannot be empty");
  }

  @Override
  public Representation toRepresentation() {
    return SQLRepresentation.builder().withDialect(dialect).withSql(sql).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SQLRepresentationDTO)) return false;
    SQLRepresentationDTO that = (SQLRepresentationDTO) o;
    return Objects.equals(dialect, that.dialect) && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dialect, sql);
  }

  @Override
  public String toString() {
    return "SQLRepresentationDTO{" + "dialect='" + dialect + '\'' + ", sql='" + sql + '\'' + '}';
  }

  /** Builder for {@link SQLRepresentationDTO}. */
  public static final class Builder {

    private String dialect;
    private String sql;

    private Builder() {}

    /**
     * Sets the dialect.
     *
     * @param dialect The dialect identifier.
     * @return This builder.
     */
    public Builder withDialect(String dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * Sets the SQL text.
     *
     * @param sql The SQL text.
     * @return This builder.
     */
    public Builder withSql(String sql) {
      this.sql = sql;
      return this;
    }

    /**
     * Builds a new {@link SQLRepresentationDTO}.
     *
     * @return The constructed instance.
     */
    public SQLRepresentationDTO build() {
      return new SQLRepresentationDTO(dialect, sql);
    }
  }
}
