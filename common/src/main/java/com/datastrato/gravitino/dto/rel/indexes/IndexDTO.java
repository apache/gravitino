/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.indexes;

import com.datastrato.gravitino.rel.indexes.Index;
import com.google.common.base.Preconditions;

public class IndexDTO implements Index {

  private final IndexType indexType;
  private final String name;
  private final String[][] fieldNames;

  public IndexDTO(IndexType indexType, String name, String[][] fieldNames) {
    this.indexType = indexType;
    this.name = name;
    this.fieldNames = fieldNames;
  }

  @Override
  public IndexType type() {
    return indexType;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String[][] fieldNames() {
    return fieldNames;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder<S extends IndexDTO.Builder> {

    protected IndexType indexType;

    protected String name;
    protected String[][] fieldNames;

    public Builder() {}

    public S withIndexType(IndexType indexType) {
      this.indexType = indexType;
      return (S) this;
    }

    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    public S withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return (S) this;
    }

    public IndexDTO build() {
      Preconditions.checkArgument(indexType != null, "Index type cannot be null");
      Preconditions.checkArgument(name != null, "Index name cannot be null");
      Preconditions.checkArgument(
          fieldNames != null, "The index must be set with corresponding column names");
      return new IndexDTO(indexType, name, fieldNames);
    }
  }
}
