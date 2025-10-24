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

import org.apache.gravitino.annotation.Evolving;

/**
 * The Index interface defines methods for implementing table index columns. Currently, settings for
 * PRIMARY_KEY and UNIQUE_KEY are provided.
 */
@Evolving
public interface Index {

  /**
   * @return The type of the index. eg: PRIMARY_KEY and UNIQUE_KEY.
   */
  IndexType type();

  /**
   * @return The name of the index.
   */
  String name();

  /**
   * @return The field name under the table contained in the index. it is the column names, could be
   *     "a.b.c" for nested column, but normally it could only be "a".
   */
  String[][] fieldNames();

  /**
   * The enum IndexType defines the type of the index. Currently, PRIMARY_KEY and UNIQUE_KEY are
   * supported.
   */
  enum IndexType {
    /**
     * PRIMARY KEY index in a relational database is a field or a combination of fields that
     * uniquely identifies each record in a table. It serves as a unique identifier for each row,
     * ensuring that no two rows have the same key. The PRIMARY KEY is used to establish
     * relationships between tables and enforce the entity integrity of a database. Additionally, it
     * helps in indexing and organizing the data for efficient retrieval and maintenance.
     */
    PRIMARY_KEY,
    /**
     * UNIQUE KEY in a relational database is a field or a combination of fields that ensures each
     * record in a table has a distinct value or combination of values. Unlike a primary key, a
     * UNIQUE KEY allows for the presence of null values, but it still enforces the constraint that
     * no two records can have the same unique key value(s). UNIQUE KEYs are used to maintain data
     * integrity by preventing duplicate entries in specific columns, and they can be applied to
     * columns that are not designated as the primary key. The uniqueness constraint imposed by
     * UNIQUE KEY helps in avoiding redundancy and ensuring data accuracy in the database.
     */
    UNIQUE_KEY,

    // The following index types are specific to Lance, for more, please see: IndexType in LanceDB
    /**
     * SCALAR index is used to optimize searches on scalar data types such as integers, floats,
     * strings, etc. Currently, this type is only applicable to Lance.
     */
    SCALAR,
    /**
     * BTREE index is a balanced tree data structure that maintains sorted data and allows for
     * logarithmic time complexity for search, insert, and delete operations. Currently, this type
     * is only applicable to Lance.
     */
    BTREE,
    /**
     * Bitmap index is a type of database index that uses bit arrays (bitmaps) to represent the
     * presence or absence of values in a column, enabling efficient querying and data retrieval.
     * Currently, this type is only applicable to Lance.
     */
    BITMAP,
    /**
     * LABEL_LIST index is used to optimize searches on columns containing lists of labels or tags.
     * Currently, this type is only applicable to Lance.
     */
    LABEL_LIST,
    /**
     * INVERTED index is a data structure used to optimize full-text searches by mapping terms to
     * their locations within a dataset, allowing for quick retrieval of documents containing
     * specific words or phrases. Currently, this type is only applicable to Lance.
     */
    INVERTED,
    /**
     * VECTOR index is used to optimize similarity searches in high-dimensional vector spaces.
     * Currently, this type is only applicable to Lance.
     */
    VECTOR,
    /** IVF_FLAT (Inverted File with Flat quantization) is an indexing method used for efficient */
    IVF_FLAT,
    /** IVF_SQ (Inverted File with Scalar Quantization) is an indexing method used for efficient */
    IVF_SQ,
    /** IVF_PQ (Inverted File with Product Quantization) is an indexing method used for efficient */
    IVF_PQ,
    /** IVF_HNSW_FLAT */
    IVF_HNSW_SQ,
    /** IVF_HNSW_PQ */
    IVF_HNSW_PQ;
  }
}
