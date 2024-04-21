/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Unstable;

/**
 * The interface of a privilege. The privilege represents the ability to execute kinds of operations
 * for kinds of entities
 */
@Unstable
public interface Privilege {

  /** @return The generic name of the privilege. */
  Name name();

  /** @return A readable string representation for the privilege. */
  String simpleString();

  /** The name of this privilege. */
  enum Name {
    /** The privilege to show catalogs. */
    SHOW_CATALOGS,
    /** The privilege to use a catalog. */
    USE_CATALOG,
    /** The privilege to create a catalog. */
    CREATE_CATALOG,
    /** The privilege to alter a catalog. */
    ALTER_CATALOG,
    /** The privilege to drop a catalog. */
    DROP_CATALOG,
    /** The privilege to show schemas. */
    SHOW_SCHEMAS,
    /** The privilege to use a schema. */
    USE_SCHEMA,
    /** The privilege to create a schema. */
    CREATE_SCHEMA,
    /** The privilege to alter a schema. */
    ALTER_SCHEMA,
    /** The privilege to drop a schema. */
    DROP_SCHEMA,
    /** The privilege to show tables. */
    SHOW_TABLES,
    /** The privilege to create a table. */
    CREATE_TABLE,
    /** The privilege to drop a table. */
    DROP_TABLE,
    /** The privilege to read a table. */
    READ_TABLE,
    /** The privilege to write a table. */
    WRITE_TABLE,
    /** The privilege to list filesets. */
    LIST_FILESETS,
    /** The privilege to create a fileset. */
    CREATE_FILESET,
    /** The privilege to drop a fileset. */
    DROP_FILESET,
    /** The privilege to read a fileset. */
    READ_FILESET,
    /** The privilege to write a fileset. */
    WRITE_FILESET,
    /** The privilege to list topics. */
    LIST_TOPICS,
    /** The privilege to create a topic. */
    CREATE_TOPIC,
    /** The privilege to drop a topic. */
    DROP_TOPIC,
    /** The privilege to read a topic. */
    READ_TOPIC,
    /** The privilege to write a topic. */
    WRITE_TOPIC,
    /** The privilege to use a metalake, the user can load the information of the metalake. */
    USE_METALAKE,
    /** The privilege to manage a metalake, including drop and alter a metalake. */
    MANAGE_METALAKE,
    /** The privilege to create a metalake. */
    CREATE_METALAKE,
    /** The privilege to manage users, including add,remove and get a user */
    MANAGE_USER,
    /** The privilege to manage groups, including add,remove and get a group. */
    MANAGE_GROUP,
    /** The privilege to manage roles, including create,drop,alter,grant and revoke a role. */
    MANAGE_ROLE,
  }
}
