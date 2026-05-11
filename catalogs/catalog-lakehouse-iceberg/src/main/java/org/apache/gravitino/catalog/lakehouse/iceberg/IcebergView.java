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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents an Apache Iceberg View entity in the Iceberg catalog. */
@ToString
@Getter
public class IcebergView implements View {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergView.class);

  private String name;
  private String comment;
  private Column[] columns;
  private Representation[] representations;
  private Map<String, String> properties;
  private AuditInfo auditInfo;

  private IcebergView() {}

  /**
   * Converts an Iceberg LoadViewResponse to a Gravitino IcebergView, extracting columns,
   * representations, and comment from the view metadata.
   *
   * @param response The Iceberg LoadViewResponse.
   * @param viewName The name of the view.
   * @return A new IcebergView instance.
   */
  public static IcebergView fromLoadViewResponse(LoadViewResponse response, String viewName) {
    ViewMetadata metadata = response.metadata();
    if (metadata == null) {
      return IcebergView.builder()
          .withName(viewName)
          .withProperties(Maps.newHashMap())
          .withColumns(new Column[0])
          .withRepresentations(new Representation[0])
          .withAuditInfo(AuditInfo.EMPTY)
          .build();
    }

    Map<String, String> properties =
        metadata.properties() != null ? Maps.newHashMap(metadata.properties()) : Maps.newHashMap();
    String comment = properties.get("comment");

    Column[] columns = extractColumns(metadata);
    Representation[] representations = extractRepresentations(metadata);

    return IcebergView.builder()
        .withName(viewName)
        .withComment(comment)
        .withColumns(columns)
        .withRepresentations(representations)
        .withProperties(properties)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

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

  /** Returns a new builder for constructing an IcebergView. */
  public static Builder builder() {
    return new Builder();
  }

  private static Column[] extractColumns(ViewMetadata metadata) {
    try {
      Schema schema = metadata.schema();
      if (schema != null && schema.columns() != null) {
        return schema.columns().stream().map(ConvertUtil::fromNestedField).toArray(Column[]::new);
      }
    } catch (Exception e) {
      LOG.warn("Failed to extract columns from Iceberg view metadata", e);
    }
    return new Column[0];
  }

  private static Representation[] extractRepresentations(ViewMetadata metadata) {
    try {
      ViewVersion currentVersion = metadata.currentVersion();
      if (currentVersion != null && currentVersion.representations() != null) {
        return currentVersion.representations().stream()
            .filter(r -> r instanceof SQLViewRepresentation)
            .map(
                r -> {
                  SQLViewRepresentation sqlRep = (SQLViewRepresentation) r;
                  return (Representation)
                      SQLRepresentation.builder()
                          .withDialect(sqlRep.dialect())
                          .withSql(sqlRep.sql())
                          .build();
                })
            .toArray(Representation[]::new);
      }
    } catch (Exception e) {
      LOG.warn("Failed to extract representations from Iceberg view metadata", e);
    }
    return new Representation[0];
  }

  /** Builder for IcebergView. */
  public static class Builder {
    private final IcebergView view;

    private Builder() {
      this.view = new IcebergView();
    }

    public Builder withName(String name) {
      view.name = name;
      return this;
    }

    public Builder withComment(String comment) {
      view.comment = comment;
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
      view.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      view.auditInfo = auditInfo;
      return this;
    }

    public IcebergView build() {
      return view;
    }
  }
}
