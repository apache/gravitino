/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.indexes;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.indexes.Index;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;

@JsonSerialize(using = JsonUtils.IndexSerializer.class)
@JsonDeserialize(using = JsonUtils.IndexDeserializer.class)
public class IndexDTO implements Index {

  public static final IndexDTO[] EMPTY_INDEXES = new IndexDTO[0];

  private IndexType indexType;
  private String name;
  private String[][] fieldNames;

  public IndexDTO() {}

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexDTO indexDTO = (IndexDTO) o;

    return indexType == indexDTO.indexType
        && Objects.equals(name, indexDTO.name)
        && compareStringArrays(fieldNames, indexDTO.fieldNames);
  }

  public static boolean compareStringArrays(String[][] array1, String[][] array2) {
    if (array1.length != array2.length) {
      return false;
    }
    for (int i = 0; i < array1.length; i++) {
      if (!Arrays.equals(array1[i], array2[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(indexType, name);
    result = 31 * result + Arrays.hashCode(fieldNames);
    return result;
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
      Preconditions.checkArgument(
          fieldNames != null && fieldNames.length > 0,
          "The index must be set with corresponding column names");
      return new IndexDTO(indexType, name, fieldNames);
    }
  }
}
