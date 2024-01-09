/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.indexes;

/** Helper methods to create index to pass into Gravitino. */
public class Indexes {

  public static final Index[] EMPTY_INDEXES = new Index[0];
  private static final String UNIQUE_KEY_FORMAT = "%s_uk";
  private static final String PRIMARY_KEY_FORMAT = "%s_pk";

  public static Index unique(String fieldName) {
    String[] fieldNames = {fieldName};
    return unique(new String[][] {fieldNames});
  }

  public static Index unique(String[][] fieldNames) {
    return unique(String.format(UNIQUE_KEY_FORMAT, fieldNames[0][0]), fieldNames);
  }

  public static Index unique(String name, String[][] fieldNames) {
    return of(Index.IndexType.UNIQUE_KEY, name, fieldNames);
  }

  public static Index primary(String fieldName) {
    String[] fieldNames = {fieldName};
    return primary(new String[][] {fieldNames});
  }

  public static Index primary(String[][] fieldNames) {
    return primary(String.format(PRIMARY_KEY_FORMAT, fieldNames[0][0]), fieldNames);
  }

  public static Index primary(String name, String[][] fieldNames) {
    return of(Index.IndexType.PRIMARY_KEY, name, fieldNames);
  }

  public static Index of(Index.IndexType indexType, String name, String[][] fieldNames) {
    return IndexImpl.builder()
        .withIndexType(indexType)
        .withName(name)
        .withFieldNames(fieldNames)
        .build();
  }

  public static final class IndexImpl implements Index {
    private final IndexType indexType;
    private final String name;
    private final String[][] fieldNames;

    public IndexImpl(IndexType indexType, String name, String[][] fieldNames) {
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

    /** Builder to create a index. */
    public static class Builder {
      protected IndexType indexType;
      protected String name;
      protected String[][] fieldNames;

      public Indexes.IndexImpl.Builder withIndexType(IndexType indexType) {
        this.indexType = indexType;
        return this;
      }

      public Indexes.IndexImpl.Builder withName(String name) {
        this.name = name;
        return this;
      }

      public Indexes.IndexImpl.Builder withFieldNames(String[][] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
      }

      public Index build() {
        return new IndexImpl(indexType, name, fieldNames);
      }
    }
  }
}
