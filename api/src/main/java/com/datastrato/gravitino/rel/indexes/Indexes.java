/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.indexes;

/** Helper methods to create index to pass into Gravitino. */
public class Indexes {

  public static final Index[] EMPTY_INDEXES = new Index[0];

  /** MySQL does not support setting the name of the primary key, so the default name is used. */
  public static final String DEFAULT_MYSQL_PRIMARY_KEY_NAME = "PRIMARY";

  /**
   * Create a unique index on columns. Like unique (a) or unique (a, b), for complex like unique
   *
   * @param name The name of the index
   * @param fieldNames The field names under the table contained in the index.
   * @return The unique index
   */
  public static Index unique(String name, String[][] fieldNames) {
    return of(Index.IndexType.UNIQUE_KEY, name, fieldNames);
  }

  /**
   * To create a MySQL primary key, you need to use the default primary key name.
   *
   * @param fieldNames The field names under the table contained in the index.
   * @return The primary key index
   */
  public static Index createMysqlPrimaryKey(String[][] fieldNames) {
    return primary(DEFAULT_MYSQL_PRIMARY_KEY_NAME, fieldNames);
  }

  /**
   * Create a primary index on columns. Like primary (a), for complex like primary
   *
   * @param name The name of the index
   * @param fieldNames The field names under the table contained in the index.
   * @return The primary index
   */
  public static Index primary(String name, String[][] fieldNames) {
    return of(Index.IndexType.PRIMARY_KEY, name, fieldNames);
  }

  /**
   * @param indexType The type of the index
   * @param name The name of the index
   * @param fieldNames The field names under the table contained in the index.
   * @return The index
   */
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
