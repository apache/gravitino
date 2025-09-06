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
package org.apache.gravitino.catalog.lakehouse.paimon;

import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.catalog.lakehouse.paimon.utils.TypeUtils;
import org.apache.gravitino.connector.BaseColumn;
import org.apache.gravitino.rel.Column;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

/** Implementation of {@link Column} that represents a column in the Apache Paimon column. */
@EqualsAndHashCode(callSuper = true)
public class GravitinoPaimonColumn extends BaseColumn {

  private GravitinoPaimonColumn() {}

  /**
   * Converts {@link GravitinoPaimonColumn} instance to inner column.
   *
   * @param id The id of inner column.
   * @param gravitinoColumn The Gravitino column to convert (contains name, type, nullability, and
   *     comment).
   * @return The converted inner column.
   */
  public static DataField toPaimonColumn(int id, Column gravitinoColumn) {
    DataType paimonType = TypeUtils.toPaimonType(gravitinoColumn.dataType());
    DataType paimonTypeWithNullable =
        gravitinoColumn.nullable() ? paimonType.nullable() : paimonType.notNull();
    return new DataField(
        id, gravitinoColumn.name(), paimonTypeWithNullable, gravitinoColumn.comment());
  }

  /**
   * Creates new {@link GravitinoPaimonColumn} instance from Paimon columns.
   *
   * @param rowType The {@link RowType} instance of Paimon column.
   * @return New {@link GravitinoPaimonColumn} instances.
   */
  public static List<GravitinoPaimonColumn> fromPaimonRowType(RowType rowType) {
    return rowType.getFields().stream()
        .map(GravitinoPaimonColumn::fromPaimonColumn)
        .collect(Collectors.toList());
  }

  /**
   * Creates a new {@link GravitinoPaimonColumn} instance from inner column.
   *
   * @param dataField The {@link DataField} instance of inner column.
   * @return A new {@link GravitinoPaimonColumn} instance.
   */
  public static GravitinoPaimonColumn fromPaimonColumn(DataField dataField) {
    return builder()
        .withName(dataField.name())
        .withType(TypeUtils.fromPaimonType(dataField.type()))
        .withComment(dataField.description())
        .withNullable(dataField.type().isNullable())
        .build();
  }

  /** A builder class for constructing {@link GravitinoPaimonColumn} instance. */
  public static class Builder extends BaseColumnBuilder<Builder, GravitinoPaimonColumn> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a {@link GravitinoPaimonColumn} instance using the provided values.
     *
     * @return A new {@link GravitinoPaimonColumn} instance with the configured values.
     */
    @Override
    protected GravitinoPaimonColumn internalBuild() {
      GravitinoPaimonColumn paimonColumn = new GravitinoPaimonColumn();
      paimonColumn.name = name;
      paimonColumn.comment = comment;
      paimonColumn.dataType = dataType;
      paimonColumn.nullable = nullable;
      paimonColumn.autoIncrement = autoIncrement;
      paimonColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      paimonColumn.auditInfo = auditInfo;
      return paimonColumn;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
