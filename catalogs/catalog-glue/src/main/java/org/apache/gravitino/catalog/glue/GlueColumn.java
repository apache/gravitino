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
package org.apache.gravitino.catalog.glue;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseColumn;
import software.amazon.awssdk.services.glue.model.Column;

/** Represents an AWS Glue {@link Column} as a Gravitino column. */
@EqualsAndHashCode(callSuper = true)
public class GlueColumn extends BaseColumn {

  private GlueColumn() {}

  /**
   * Converts an AWS Glue {@link Column} to a {@link GlueColumn}.
   *
   * <p>Field mapping:
   *
   * <ul>
   *   <li>{@code Column.name()} → {@code name}
   *   <li>{@code Column.type()} → {@code dataType} via {@link GlueTypeConverter#toGravitino}
   *   <li>{@code Column.comment()} → {@code comment} (nullable)
   *   <li>Glue has no nullability metadata → {@code nullable = true} always
   *   <li>Glue has no auto-increment concept → {@code autoIncrement = false} always
   * </ul>
   *
   * @param glueColumn the Glue Column returned by the AWS SDK
   * @return a populated {@link GlueColumn}
   */
  public static GlueColumn fromGlueColumn(Column glueColumn) {
    return GlueColumn.builder()
        .withName(glueColumn.name())
        .withType(GlueTypeConverter.CONVERTER.toGravitino(glueColumn.type()))
        .withComment(glueColumn.comment())
        .withNullable(true)
        .build();
  }

  /** Builder for {@link GlueColumn}. */
  public static class Builder extends BaseColumnBuilder<Builder, GlueColumn> {

    private Builder() {}

    @Override
    protected GlueColumn internalBuild() {
      GlueColumn col = new GlueColumn();
      col.name = name;
      col.comment = comment;
      col.dataType = dataType;
      col.nullable = nullable;
      col.autoIncrement = autoIncrement;
      col.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      return col;
    }
  }

  /**
   * Creates a new {@link Builder}.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }
}
