/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.meta.rel.BaseColumn;

public class HiveColumn extends BaseColumn {

  private HiveColumn() {}

  public static class Builder extends BaseColumnBuilder<Builder, HiveColumn> {

    @Override
    protected HiveColumn internalBuild() {
      HiveColumn hiveColumn = new HiveColumn();

      hiveColumn.name = name;
      hiveColumn.comment = comment;
      hiveColumn.dataType = dataType;
      return hiveColumn;
    }
  }
}
