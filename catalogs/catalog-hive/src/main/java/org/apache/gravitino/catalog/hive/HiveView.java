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
package org.apache.gravitino.catalog.hive;

import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;

/**
 * Represents a view stored in Hive Metastore (VIRTUAL_VIEW table type). The SQL dialect is detected
 * from table properties: Trino views start with "/* Presto View:", Spark views carry {@code
 * spark.sql.create.version} in their parameters, and all other views are treated as native Hive SQL
 * views.
 */
@Unstable
@EqualsAndHashCode
@ToString
public class HiveView implements View {

  static final String HIVE_DIALECT = "hive";
  static final String SPARK_DIALECT = "spark";
  static final String TRINO_DIALECT = "trino";

  private static final String SPARK_VERSION_KEY = "spark.sql.create.version";
  private static final String TRINO_VIEW_MARKER_KEY = "presto_view";
  private static final String TRINO_VIEW_PREFIX = "/* Presto View:";

  private String name;
  private String comment;
  private Column[] columns;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private SQLRepresentation[] representations;

  private HiveView() {}

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
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Detects the SQL dialect from HMS table properties and view text.
   *
   * @param viewOriginalText The original view text from HMS.
   * @param parameters The HMS table parameters map.
   * @return The detected dialect string: "trino", "spark", or "hive".
   */
  static String detectDialect(String viewOriginalText, Map<String, String> parameters) {
    if (parameters != null && "true".equalsIgnoreCase(parameters.get(TRINO_VIEW_MARKER_KEY))) {
      return TRINO_DIALECT;
    }
    if (viewOriginalText != null && viewOriginalText.startsWith(TRINO_VIEW_PREFIX)) {
      return TRINO_DIALECT;
    }
    if (parameters != null && parameters.containsKey(SPARK_VERSION_KEY)) {
      return SPARK_DIALECT;
    }
    return HIVE_DIALECT;
  }

  /**
   * Returns a new {@link Builder} for constructing a {@code HiveView}.
   *
   * @return A fresh builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link HiveView}. */
  public static class Builder {
    private final HiveView view;

    private Builder() {
      view = new HiveView();
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
     * Builds the {@link HiveView} instance.
     *
     * @return The constructed view.
     */
    public HiveView build() {
      return view;
    }
  }
}
