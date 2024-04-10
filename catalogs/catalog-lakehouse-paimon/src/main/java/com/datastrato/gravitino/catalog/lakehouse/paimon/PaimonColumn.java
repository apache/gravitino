/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.fromPaimonType;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TypeUtils.toPaimonType;

import com.datastrato.gravitino.connector.BaseColumn;
import com.datastrato.gravitino.rel.Column;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

/** Implementation of {@link Column} that represents a column in the Paimon column. */
@EqualsAndHashCode(callSuper = true)
public class PaimonColumn extends BaseColumn {

  private PaimonColumn() {}

  /**
   * Converts {@link PaimonColumn} instance to inner column.
   *
   * @param id The id of inner column.
   * @return The converted inner column.
   */
  public DataField toPaimonColumn(int id) {
    return new DataField(id, name, toPaimonType(dataType), comment);
  }

  /**
   * Creates new {@link PaimonColumn} instance from inner columns.
   *
   * @param rowType The {@link RowType} instance of inner column.
   * @return New {@link PaimonColumn} instances.
   */
  public static List<PaimonColumn> fromPaimonColumn(RowType rowType) {
    return rowType.getFields().stream()
        .map(PaimonColumn::fromPaimonColumn)
        .collect(Collectors.toList());
  }

  /**
   * Creates a new {@link PaimonColumn} instance from inner column.
   *
   * @param dataField The {@link DataField} instance of inner column.
   * @return A new {@link PaimonColumn} instance.
   */
  public static PaimonColumn fromPaimonColumn(DataField dataField) {
    return builder()
        .withName(dataField.name())
        .withType(fromPaimonType(dataField.type()))
        .withComment(dataField.description())
        .withNullable(dataField.type().isNullable())
        .build();
  }

  /** A builder class for constructing {@link PaimonColumn} instance. */
  public static class Builder extends BaseColumnBuilder<Builder, PaimonColumn> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a {@link PaimonColumn} instance using the provided values.
     *
     * @return A new {@link PaimonColumn} instance with the configured values.
     */
    @Override
    protected PaimonColumn internalBuild() {
      PaimonColumn paimonColumn = new PaimonColumn();
      paimonColumn.name = name;
      paimonColumn.comment = comment;
      paimonColumn.dataType = dataType;
      paimonColumn.nullable = nullable;
      paimonColumn.autoIncrement = autoIncrement;
      paimonColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
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
