/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

/**
 * The type of securable object in the Gravitino system. Every type will map one kind of the entity
 * of the underlying system.
 */
public enum SecurableObjectType {
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
  /** A table is mapped the table of relational data sources like Apache Hive, MySQL, etc */
  TABLE,
  /** A topic is mapped the topic of messaging data sources like Apache Kafka, Apache Pulsar, etc */
  TOPIC
}
