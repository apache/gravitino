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
package org.apache.gravitino.catalog.fluss;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import org.apache.fluss.metadata.Schema;
import org.apache.gravitino.connector.BaseColumn;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;

/** Fluss column representation. */
@EqualsAndHashCode(callSuper = true)
public class FlussColumn extends BaseColumn {

  private FlussColumn() {}

  /**
   * Creates a builder for {@link FlussColumn}.
   *
   * @return the column builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Converts a Fluss schema column to a Gravitino column.
   *
   * @param column the Fluss schema column
   * @return the converted Gravitino column
   */
  public static FlussColumn fromFlussColumn(Schema.Column column) {
    return FlussColumn.builder()
        .withName(column.getName())
        .withDataType(FlussDataTypeConverter.CONVERTER.toGravitino(column.getDataType()))
        .withComment(column.getComment().orElse(null))
        .withNullable(column.getDataType().isNullable())
        .build();
  }

  /**
   * Converts a Gravitino column to a Fluss schema column.
   *
   * @param column the Gravitino column
   * @return the converted Fluss schema column
   */
  public static Schema.Column toFlussColumn(Column column) {
    if (column.defaultValue() != Column.DEFAULT_VALUE_NOT_SET) {
      throw new IllegalArgumentException("Fluss does not support column default values");
    }

    return new Schema.Column(
        column.name(),
        FlussDataTypeConverter.CONVERTER.fromGravitino(column.dataType(), column.nullable()),
        column.comment());
  }

  /** Builder for {@link FlussColumn}. */
  public static class Builder extends BaseColumnBuilder<Builder, FlussColumn> {
    @Override
    protected FlussColumn internalBuild() {
      FlussColumn c = new FlussColumn();
      c.name = Preconditions.checkNotNull(name, "name");
      c.comment = comment;
      c.dataType = Preconditions.checkNotNull(dataType, "dataType");
      c.nullable = nullable;
      c.autoIncrement = false;
      c.defaultValue = defaultValue == null ? Column.DEFAULT_VALUE_NOT_SET : defaultValue;
      return c;
    }

    /** Optional column comment (Fluss does not enforce). */
    public Builder withComment(@Nullable String c) {
      this.comment = c;
      return this;
    }

    /** Column data type. */
    public Builder withDataType(Type t) {
      this.dataType = t;
      return this;
    }
  }
}
