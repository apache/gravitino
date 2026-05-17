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

  @Override
  public String comment() {
    return comment;
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

    public Builder withName(String name) {
      view.name = name;
      return this;
    }

    public Builder withComment(String comment) {
      view.comment = comment;
      return this;
    }

    public Builder withDefaultCatalog(String defaultCatalog) {
      view.defaultCatalog = defaultCatalog;
      return this;
    }

    public Builder withDefaultSchema(String defaultSchema) {
      view.defaultSchema = defaultSchema;
      return this;
    }

    public Builder withColumns(Column[] columns) {
      view.columns = columns;
      return this;
    }

    public Builder withRepresentations(Representation[] representations) {
      view.representations = representations;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      view.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      view.auditInfo = auditInfo;
      return this;
    }

    public JdbcView build() {
      return view;
    }
  }
}
