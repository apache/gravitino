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

package org.apache.gravitino;

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Unstable;

/**
 * The MetadataObject is the basic unit of the Gravitino system. It represents the metadata object
 * in the Apache Gravitino system. The object can be a metalake, catalog, schema, table, topic, etc.
 */
@Unstable
public interface MetadataObject {
  /**
   * The type of object in the Gravitino system. Every type will map one kind of the entity of the
   * underlying system.
   */
  enum Type {
    /**
     * A metalake is a concept of tenant. It means an organization. A metalake contains many data
     * sources.
     */
    METALAKE,
    /**
     * A catalog is a collection of metadata from a specific metadata source, like Apache Hive
     * catalog, Apache Iceberg catalog, JDBC catalog, etc.
     */
    CATALOG,
    /**
     * A schema is a sub collection of the catalog. The schema can contain filesets, tables, topics,
     * etc.
     */
    SCHEMA,
    /** A fileset is mapped to a directory on a file system like HDFS, S3, ADLS, GCS, etc. */
    FILESET,
    /** A table is mapped the table of relational data sources like Apache Hive, MySQL, etc. */
    TABLE,
    /**
     * A topic is mapped the topic of messaging data sources like Apache Kafka, Apache Pulsar, etc.
     */
    TOPIC,
    /** A column is a sub-collection of the table that represents a group of same type data. */
    COLUMN,
    /** A role is an object contains specific securable objects with privileges */
    ROLE,
    /** A model is mapped to the model artifact in ML. */
    MODEL,
    /** A tag is used to help manage other metadata object. */
    TAG;
  }

  /**
   * The parent full name of the object. If the object doesn't have parent, this method will return
   * null.
   *
   * @return The parent full name of the object.
   */
  @Nullable
  String parent();

  /**
   * The name of the object.
   *
   * @return The name of the object.
   */
  String name();

  /**
   * The full name of the object. Full name will be separated by "." to represent a string
   * identifier of the object, like catalog, catalog.table, etc.
   *
   * @return The name of the object.
   */
  default String fullName() {
    if (parent() == null) {
      return name();
    } else {
      return parent() + "." + name();
    }
  }

  /**
   * The type of the object.
   *
   * @return The type of the object.
   */
  Type type();
}
