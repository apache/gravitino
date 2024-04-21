/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Unstable;
import javax.annotation.Nullable;

/**
 * The securable object is the entity which access can be granted. Unless allowed by a grant, access
 * is denied. Gravitino organizes the securable objects using tree structure. The securable object
 * may be a catalog, a table or a schema, etc. <br>
 * For example, `catalog1.schema1.table1` represents a table named `table1`. It's in the schema
 * named `schema1`. The schema is in the catalog named `catalog1`. <br>
 * Similarly, `catalog1.schema1.topic1` can represent a topic.`catalog1.schema1.fileset1` can
 * represent a fileset.<br>
 * `*` can represnet all th metalakes <br>
 * `metalake` can represent all the catalogs of this metalake. <br>
 * To use other securable objects which represents all entities," you can use their parent entity,
 * For example if you want to have read table privileges of all tables of `catalog1.schema1`, " you
 * can use add `read table` privilege for `catalog1.schema1` directly
 */
@Unstable
public interface SecurableObject {

  /**
   * The parent securable object. If the securable object doesn't have parent, this method will
   * return null.
   *
   * @return The parent securable object.
   */
  @Nullable
  SecurableObject parent();

  /**
   * The name of th securable object.
   *
   * @return The name of the securable object.
   */
  String name();

  /**
   * The full name of th securable object. If the parent isn't null, the full name will join the
   * parent full name and the name with `.`, otherwise will return the name.
   *
   * @return The name of the securable object.
   */
  String fullName();

  /**
   * The type of securable object
   *
   * @return The type of securable object.
   */
  Type type();

  /**
   * The type of securable object in the Gravitino system. Every type will map one kind of the
   * entity of the underlying system.
   */
  enum Type {
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
    /**
     * A metalake is a concept of tenant. It means an organization. A metalake contains many data
     * sources.
     */
    METALAKE
  }
}
