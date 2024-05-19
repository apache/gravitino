/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Unstable;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The securable object is the entity which access can be granted. Unless allowed by a grant, access
 * is denied. Gravitino organizes the securable objects using tree structure. <br>
 * There are three fields in the securable object: parent, name, and type. <br>
 * The types include 6 kinds: CATALOG,SCHEMA,TABLE,FILESET,TOPIC and METALAKE. <br>
 * You can use the helper class `SecurableObjects` to create the securable object which you need.
 * <br>
 * You can use full name and type of the securable object in the RESTFUL API. <br>
 * For example, <br>
 * If you want to use a catalog named `catalog1`, you can use the code
 * `SecurableObjects.ofCatalog("catalog1")` to create the securable object, or you can use full name
 * `catalog1` and type `CATALOG` in the RESTFUL API. <br>
 * If you want to use a schema named `schema1` in the catalog named `catalog1`, you can use the code
 * `SecurableObjects.ofSchema(catalog, "schema1")` to create the securable object, or you can use
 * full name `catalog1.schema1` and type `SCHEMA` in the RESTFUL API. <br>
 * If you want to use a table named `table1` in the schema named `schema1`, you can use the code
 * `SecurableObjects.ofTable(schema, "table1")` to create the securable object, or you can use full
 * name `catalog1.schema1.table1` and type `TABLE` in the RESTFUL API. <br>
 * If you want to use a topic named `topic1` in the schema named `schema1`, you can use the code
 * `SecurableObjects.ofTopic(schema, "topic1")` to create the securable object, or you can use full
 * name `catalog1.schema1.topic1` and type `TOPIC` in the RESTFUL API. <br>
 * If you want to use a fileset named `fileset1` in the schema named `schema1`, you can use the code
 * `SecurableObjects.ofFileset(schema, "fileset1)` to create the securable object, or you can use
 * full name `catalog1.schema1.fileset1` and type `FILESET` in the RESTFUL API. <br>
 * If you want to use a metalake named `metalake1`, you can use the code
 * `SecurableObjects.ofMetalake("metalake1")` to create the securable object, or you can use full
 * name `metalake1` and type `METALAKE` in the RESTFUL API. <br>
 * If you want to use all the catalogs, you use the metalake to represent them. Likely, you can use
 * their common parent to represent all securable objects.<br>
 * For example if you want to have read table privileges of all tables of `catalog1.schema1`, " you
 * can use add `read table` privilege for `catalog1.schema1` directly
 */
@Unstable
public interface SecurableObject {

  /**
   * The parent full name of securable object. If the securable object doesn't have parent, this
   * method will return null.
   *
   * @return The parent full name of securable object.
   */
  @Nullable
  String parentFullName();

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
   * The privileges of the securable object. For example: If the securable object is a table, the
   * privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a schema has the privilege of `LOAD
   * TABLE`. It means the role can all tables of the schema.
   *
   * @return The privileges of the role.
   */
  List<Privilege> privileges();

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
