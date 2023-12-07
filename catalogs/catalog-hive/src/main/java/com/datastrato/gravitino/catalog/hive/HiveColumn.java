/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.catalog.rel.BaseColumn;
import lombok.EqualsAndHashCode;

/** Represents a column in the Hive Metastore catalog. */
@EqualsAndHashCode(callSuper = true)
public class HiveColumn extends BaseColumn {

  private HiveColumn() {}

  /** A builder class for constructing HiveColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, HiveColumn> {

    /**
     * Internal method to build a HiveColumn instance using the provided values.
     *
     * @return A new HiveColumn instance with the configured values.
     */
    @Override
    protected HiveColumn internalBuild() {
      HiveColumn hiveColumn = new HiveColumn();

      hiveColumn.name = name;
      hiveColumn.comment = comment;
      hiveColumn.dataType = dataType;
      hiveColumn.nullable = nullable;
      return hiveColumn;
    }
  }
}
