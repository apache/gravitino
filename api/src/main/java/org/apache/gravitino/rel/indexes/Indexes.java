/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.rel.indexes;

/** Helper methods to create index to pass into Apache Gravitino. */
public class Indexes {

  /** An empty array of indexes. */
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

  /** The user side implementation of the index. */
  public static final class IndexImpl implements Index {
    private final IndexType indexType;
    private final String name;
    private final String[][] fieldNames;

    /**
     * The constructor of the index.
     *
     * @param indexType The type of the index
     * @param name The name of the index
     * @param fieldNames The field names under the table contained in the index.
     */
    private IndexImpl(IndexType indexType, String name, String[][] fieldNames) {
      this.indexType = indexType;
      this.name = name;
      this.fieldNames = fieldNames;
    }

    /**
     * @return The type of the index
     */
    @Override
    public IndexType type() {
      return indexType;
    }

    /**
     * @return The name of the index
     */
    @Override
    public String name() {
      return name;
    }

    /**
     * @return The field names under the table contained in the index
     */
    @Override
    public String[][] fieldNames() {
      return fieldNames;
    }

    /**
     * @return the builder for creating a new instance of IndexImpl.
     */
    public static Builder builder() {
      return new Builder();
    }

    /** Builder to create an index. */
    public static class Builder {

      /** The type of the index. */
      protected IndexType indexType;

      /** The name of the index. */
      protected String name;

      /** The field names of the index. */
      protected String[][] fieldNames;

      /**
       * Set the type of the index.
       *
       * @param indexType The type of the index
       * @return The builder for creating a new instance of IndexImpl.
       */
      public Indexes.IndexImpl.Builder withIndexType(IndexType indexType) {
        this.indexType = indexType;
        return this;
      }

      /**
       * Set the name of the index.
       *
       * @param name The name of the index
       * @return The builder for creating a new instance of IndexImpl.
       */
      public Indexes.IndexImpl.Builder withName(String name) {
        this.name = name;
        return this;
      }

      /**
       * Set the field names of the index.
       *
       * @param fieldNames The field names of the index
       * @return The builder for creating a new instance of IndexImpl.
       */
      public Indexes.IndexImpl.Builder withFieldNames(String[][] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
      }

      /**
       * Build a new instance of IndexImpl.
       *
       * @return The new instance.
       */
      public Index build() {
        return new IndexImpl(indexType, name, fieldNames);
      }
    }
  }

  private Indexes() {}
}
