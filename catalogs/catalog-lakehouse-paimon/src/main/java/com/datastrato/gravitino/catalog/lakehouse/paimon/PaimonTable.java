/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonColumn.fromPaimonColumn;
import static com.datastrato.gravitino.meta.AuditInfo.EMPTY;

import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.ToString;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;

/** Implementation of {@link Table} that represents a Paimon Table entity in the Paimon table. */
@ToString
@Getter
public class PaimonTable extends BaseTable {

  private PaimonTable() {}

  @Override
  protected TableOperations newOps() {
    // TODO: Implement this interface when we have the Paimon table operations.
    throw new UnsupportedOperationException("PaimonTable does not support TableOperations.");
  }

  /**
   * Converts {@link PaimonTable} instance to inner table.
   *
   * @return The converted inner table.
   */
  public Pair<String, Schema> toPaimonTable(String tableName) {
    Schema.Builder builder = Schema.newBuilder().comment(comment).options(properties);
    for (int index = 0; index < columns.length; index++) {
      DataField dataField = ((PaimonColumn) columns[index]).toPaimonColumn(index);
      builder.column(dataField.name(), dataField.type(), dataField.description());
    }
    return Pair.of(tableName, builder.build());
  }

  /**
   * Creates a new {@link PaimonTable} instance from inner table.
   *
   * @param table The {@link Table} instance of inner table.
   * @return A new {@link PaimonTable} instance.
   */
  public static PaimonTable fromPaimonTable(Table table) {
    return builder()
        .withName(table.name())
        .withColumns(fromPaimonColumn(table.rowType()).toArray(new PaimonColumn[0]))
        .withComment(table.comment().orElse(null))
        .withProperties(table.options())
        .withAuditInfo(EMPTY)
        .build();
  }

  /** A builder class for constructing {@link PaimonTable} instance. */
  public static class Builder extends BaseTableBuilder<Builder, PaimonTable> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a {@link PaimonTable} instance using the provided values.
     *
     * @return A new {@link PaimonTable} instance with the configured values.
     */
    @Override
    protected PaimonTable internalBuild() {
      PaimonTable paimonTable = new PaimonTable();
      paimonTable.name = name;
      paimonTable.comment = comment;
      paimonTable.columns = columns;
      paimonTable.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
      paimonTable.auditInfo = auditInfo;
      return paimonTable;
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
