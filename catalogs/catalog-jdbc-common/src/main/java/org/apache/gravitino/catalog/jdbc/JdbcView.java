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
package org.apache.gravitino.catalog.jdbc;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;

/** Represents a view stored in a JDBC-compatible database. */
@Unstable
@EqualsAndHashCode
@ToString
public class JdbcView implements View {

  private String name;
  private String comment;
  private Column[] columns;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private SQLRepresentation[] representations;
  private String defaultCatalog;
  private String defaultSchema;

  private JdbcView() {}

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Column[] columns() {
    return columns == null ? new Column[0] : columns;
  }

  @Override
  public Representation[] representations() {
    return representations == null ? new SQLRepresentation[0] : representations;
  }

  @Override
  public String defaultCatalog() {
    return defaultCatalog;
  }

  @Override
  public String defaultSchema() {
    return defaultSchema;
  }

  @Override
  public Map<String, String> properties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns a new {@link Builder} for constructing a {@code JdbcView}.
   *
   * @return A fresh builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link JdbcView}. */
  public static class Builder {
    private final JdbcView view;

    private Builder() {
      view = new JdbcView();
    }

    /**
     * Sets the view name.
     *
     * @param name The view name.
     * @return This builder.
     */
    public Builder withName(String name) {
      view.name = name;
      return this;
    }

    /**
     * Sets the view comment.
     *
     * @param comment The view comment.
     * @return This builder.
     */
    public Builder withComment(String comment) {
      view.comment = comment;
      return this;
    }

    /**
     * Sets the output columns.
     *
     * @param columns The view output columns.
     * @return This builder.
     */
    public Builder withColumns(Column[] columns) {
      view.columns = columns;
      return this;
    }

    /**
     * Sets the view properties.
     *
     * @param properties The properties map.
     * @return This builder.
     */
    public Builder withProperties(Map<String, String> properties) {
      view.properties = properties;
      return this;
    }

    /**
     * Sets the audit info.
     *
     * @param auditInfo The audit info.
     * @return This builder.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      view.auditInfo = auditInfo;
      return this;
    }

    /**
     * Sets the SQL representations.
     *
     * @param representations The representations array.
     * @return This builder.
     */
    public Builder withRepresentations(SQLRepresentation[] representations) {
      view.representations = representations;
      return this;
    }

    /**
     * Sets the default catalog for unqualified identifier resolution.
     *
     * @param defaultCatalog The default catalog name, may be {@code null}.
     * @return This builder.
     */
    public Builder withDefaultCatalog(String defaultCatalog) {
      view.defaultCatalog = defaultCatalog;
      return this;
    }

    /**
     * Sets the default schema for unqualified identifier resolution.
     *
     * @param defaultSchema The default schema name, may be {@code null}.
     * @return This builder.
     */
    public Builder withDefaultSchema(String defaultSchema) {
      view.defaultSchema = defaultSchema;
      return this;
    }

    /**
     * Builds the {@link JdbcView} instance.
     *
     * @return The constructed view.
     */
    public JdbcView build() {
      return view;
    }
  }
}
