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

/** Data transfer object representing index information. */
@JsonSerialize(using = JsonUtils.IndexSerializer.class)
@JsonDeserialize(using = JsonUtils.IndexDeserializer.class)
public class IndexDTO implements Index {

  /** An empty array of indexes. */
  public static final IndexDTO[] EMPTY_INDEXES = new IndexDTO[0];

  private IndexType indexType;
  private String name;
  private String[][] fieldNames;

  /** Default constructor for Jackson deserialization. */
  public IndexDTO() {}

  /**
   * Creates a new instance of IndexDTO
   *
   * @param indexType The type of the index.
   * @param name The name of the index.
   * @param fieldNames The names of the fields.
   */
  public IndexDTO(IndexType indexType, String name, String[][] fieldNames) {
    this.indexType = indexType;
    this.name = name;
    this.fieldNames = fieldNames;
  }

  /** @return The type of the index. */
  @Override
  public IndexType type() {
    return indexType;
  }

  /** @return The name of the index. */
  @Override
  public String name() {
    return name;
  }

  /** @return The field name under the table contained in the index. */
  @Override
  public String[][] fieldNames() {
    return fieldNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IndexDTO)) return false;
    IndexDTO indexDTO = (IndexDTO) o;

    return indexType == indexDTO.indexType
        && Objects.equals(name, indexDTO.name)
        && compareStringArrays(fieldNames, indexDTO.fieldNames);
  }

  private static boolean compareStringArrays(String[][] array1, String[][] array2) {
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

  /**
   * Get the builder for creating a new instance of IndexDTO.
   *
   * @return The builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating a new instance of IndexDTO. */
  public static class Builder<S extends IndexDTO.Builder> {

    /** The type of the index. */
    protected IndexType indexType;

    /** The name of the index. */
    protected String name;
    /** The names of the fields. */
    protected String[][] fieldNames;

    /** Default constructor. */
    public Builder() {}

    /**
     * Sets the type of the index.
     *
     * @param indexType The type of the index.
     * @return The builder.
     */
    public S withIndexType(IndexType indexType) {
      this.indexType = indexType;
      return (S) this;
    }

    /**
     * Sets the name of the index.
     *
     * @param name The name of the index.
     * @return The builder.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the field names of the index.
     *
     * @param fieldNames The field names of the index.
     * @return The builder.
     */
    public S withFieldNames(String[][] fieldNames) {
      this.fieldNames = fieldNames;
      return (S) this;
    }

    /**
     * Builds a new instance of IndexDTO.
     *
     * @return The new instance.
     */
    public IndexDTO build() {
      Preconditions.checkArgument(indexType != null, "Index type cannot be null");
      Preconditions.checkArgument(
          fieldNames != null && fieldNames.length > 0,
          "The index must be set with corresponding column names");
      return new IndexDTO(indexType, name, fieldNames);
    }
  }
}
