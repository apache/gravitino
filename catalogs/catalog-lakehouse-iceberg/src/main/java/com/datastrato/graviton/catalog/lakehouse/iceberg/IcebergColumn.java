/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.catalog.rel.BaseColumn;

/** Represents a column in the Iceberg Metastore catalog. */
public class IcebergColumn extends BaseColumn {

  private int id;

  private boolean optional;

  private IcebergColumn() {}

  public int getId() {
    return id;
  }

  public boolean isOptional() {
    return optional;
  }

  /** A builder class for constructing IcebergColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, IcebergColumn> {

    /** The ID of this field. */
    private int id;

    /** Can the corresponding value of this field be null. */
    private boolean optional;

    public Builder withId(int id) {
      this.id = id;
      return this;
    }

    public Builder withOptional(boolean optional) {
      this.optional = optional;
      return this;
    }

    /**
     * Internal method to build a IcebergColumn instance using the provided values.
     *
     * @return A new IcebergColumn instance with the configured values.
     */
    @Override
    protected IcebergColumn internalBuild() {
      IcebergColumn icebergColumn = new IcebergColumn();
      icebergColumn.id = id;
      icebergColumn.name = name;
      icebergColumn.comment = comment;
      icebergColumn.dataType = dataType;
      icebergColumn.optional = optional;
      return icebergColumn;
    }
  }
}
