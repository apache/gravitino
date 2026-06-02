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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;

/**
 * Represents a view stored in Hive Metastore (VIRTUAL_VIEW table type). The SQL dialect is detected
 * from table properties: Trino views start with "/* Presto View:", Spark views carry {@code
 * spark.sql.create.version}, Flink views carry properties prefixed with {@code flink.}, and all
 * other views are treated as native Hive SQL views.
 */
@Unstable
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with", builderClassName = "Builder")
@EqualsAndHashCode
@ToString
public class HiveView implements View {

  private static final String SPARK_VERSION_KEY = "spark.sql.create.version";
  static final String SPARK_DEFAULT_CATALOG_KEY = "gravitino.view.default.catalog";
  static final String SPARK_DEFAULT_SCHEMA_KEY = "gravitino.view.default.schema";
  static final String FLINK_PROPERTY_PREFIX = "flink.";
  private static final String TRINO_VIEW_MARKER_KEY = "presto_view";
  private static final String TRINO_VIEW_PREFIX = "/* Presto View:";

  private String name;
  private String comment;
  private Column[] columns;
  private String defaultCatalog;
  private String defaultSchema;
  private Map<String, String> properties;
  private AuditInfo auditInfo;
  private SQLRepresentation[] representations;

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
  @Nullable
  public String defaultCatalog() {
    return defaultCatalog;
  }

  @Override
  @Nullable
  public String defaultSchema() {
    return defaultSchema;
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
   * @return The detected dialect string: "trino", "spark", "flink", or "hive".
   */
  static String detectDialect(String viewOriginalText, Map<String, String> parameters) {
    if (parameters != null && "true".equalsIgnoreCase(parameters.get(TRINO_VIEW_MARKER_KEY))) {
      return Dialects.TRINO;
    }
    if (StringUtils.startsWith(viewOriginalText, TRINO_VIEW_PREFIX)) {
      return Dialects.TRINO;
    }
    if (parameters != null && parameters.containsKey(SPARK_VERSION_KEY)) {
      return Dialects.SPARK;
    }
    if (parameters != null
        && parameters.keySet().stream().anyMatch(k -> k.startsWith(FLINK_PROPERTY_PREFIX))) {
      return Dialects.FLINK;
    }
    return Dialects.HIVE;
  }

  /**
   * Returns true if {@code properties} contains the Spark dialect marker key {@code
   * spark.sql.create.version}, which {@link #detectDialect} requires to identify a view as Spark.
   */
  static boolean hasSparkMarker(Map<String, String> properties) {
    return properties.containsKey(SPARK_VERSION_KEY);
  }

  /** Builder for {@link HiveView}. */
  public static class Builder {
    /**
     * Builds the {@link HiveView} instance.
     *
     * @return The constructed view.
     */
    public HiveView build() {
      Preconditions.checkArgument(StringUtils.isNotEmpty(name), "View name is required");
      Preconditions.checkArgument(
          ArrayUtils.isNotEmpty(representations), "Representations must not be null or empty");
      for (SQLRepresentation representation : representations) {
        Preconditions.checkArgument(
            representation != null, "Representations must not contain null elements");
      }

      Map<String, String> normalizedProperties =
          properties == null ? new HashMap<>() : new HashMap<>(properties);
      return new HiveView(
          name,
          comment,
          columns,
          defaultCatalog,
          defaultSchema,
          normalizedProperties,
          auditInfo,
          representations);
    }
  }
}
