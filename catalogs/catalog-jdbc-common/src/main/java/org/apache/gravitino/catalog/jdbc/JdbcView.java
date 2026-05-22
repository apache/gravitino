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

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.ToString;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;

/** Represents a JDBC view entity discovered from an underlying relational database. */
@ToString
public class JdbcView implements View {

  private String name;
  private String comment;
  private String defaultCatalog;
  private String defaultSchema;
  private Column[] columns;
  private Representation[] representations;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private JdbcView() {}

  @Override
  public String name() {
    return name;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Nullable
  @Override
  public String defaultCatalog() {
    return defaultCatalog;
  }

  @Nullable
  @Override
  public String defaultSchema() {
    return defaultSchema;
  }

  @Override
  public Column[] columns() {
    return columns != null ? columns : new Column[0];
  }

  @Override
  public Representation[] representations() {
    return representations != null ? representations : new Representation[0];
  }

  @Override
  public Map<String, String> properties() {
    return properties != null ? properties : Collections.emptyMap();
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /** Returns a new builder for constructing a {@link JdbcView}. */
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
     * Sets the name of the view.
     *
     * @param name The view name.
     * @return This builder instance.
     */
    public Builder withName(String name) {
      view.name = name;
      return this;
    }

    /**
     * Sets the comment for the view.
     *
     * @param comment The comment, or {@code null} if not set.
     * @return This builder instance.
     */
    public Builder withComment(String comment) {
      view.comment = comment;
      return this;
    }

    /**
     * Sets the default catalog used to resolve unqualified identifiers in the view definition.
     *
     * @param defaultCatalog The default catalog, or {@code null} if not applicable.
     * @return This builder instance.
     */
    public Builder withDefaultCatalog(String defaultCatalog) {
      view.defaultCatalog = defaultCatalog;
      return this;
    }

    /**
     * Sets the default schema used to resolve unqualified identifiers in the view definition.
     *
     * @param defaultSchema The default schema, or {@code null} if not applicable.
     * @return This builder instance.
     */
    public Builder withDefaultSchema(String defaultSchema) {
      view.defaultSchema = defaultSchema;
      return this;
    }

    /**
     * Sets the output columns of the view.
     *
     * @param columns The view output columns.
     * @return This builder instance.
     */
    public Builder withColumns(Column[] columns) {
      view.columns = columns;
      return this;
    }

    /**
     * Sets the SQL representations of the view.
     *
     * @param representations The view representations.
     * @return This builder instance.
     */
    public Builder withRepresentations(Representation[] representations) {
      view.representations = representations;
      return this;
    }

    /**
     * Sets the properties of the view.
     *
     * @param properties The properties map, or {@code null} to use an empty map.
     * @return This builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      view.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
      return this;
    }

    /**
     * Sets the audit information for the view.
     *
     * @param auditInfo The audit information.
     * @return This builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      view.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds and returns the {@link JdbcView} instance.
     *
     * @return The constructed {@link JdbcView}.
     */
    public JdbcView build() {
      return view;
    }
  }
}
