/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Unstable;
import javax.annotation.Nullable;

/**
 * The MetadataObject is the basic unit of the Gravitino system. It represents the metadata object
 * in the Gravitino system. The object can be a metalake, catalog, schema, table, topic, etc.
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
    COLUMN
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
   * The name of th object.
   *
   * @return The name of the object.
   */
  String name();

  /**
   * The full name of th object. Full name will be separated by "." to represent a string identifier
   * of the object, like catalog, catalog.table, etc.
   *
   * @return The name of the object.
   */
  String fullName();

  /**
   * The type of the object.
   *
   * @return The type of the object.
   */
  Type type();
}
