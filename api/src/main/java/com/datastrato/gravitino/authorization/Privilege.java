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
    /** The privilege to use a catalog. */
    USE_CATALOG(1L),
    /** The privilege to create a catalog. */
    CREATE_CATALOG(1L << 1),
    /** The privilege to alter a catalog. */
    ALTER_CATALOG(1L << 2),
    /** The privilege to drop a catalog. */
    DROP_CATALOG(1L << 3),
    /** the privilege to use a schema. */
    USE_SCHEMA(1L << 3),
    /** The privilege to create a schema. */
    CREATE_SCHEMA(1L << 4),
    /** The privilege to alter a schema. */
    ALTER_SCHEMA(1L << 5),
    /** The privilege to drop a schema. */
    DROP_SCHEMA(1L << 6),
    /** The privilege to create a table. */
    CREATE_TABLE(1L << 7),
    /** The privilege to drop a table. */
    DROP_TABLE(1L << 8),
    /** The privilege to read a table. */
    READ_TABLE(1L << 9),
    /** The privilege to write a table. */
    WRITE_TABLE(1L << 10),
    /** The privilege to create a fileset. */
    CREATE_FILESET(1L << 11),
    /** The privilege to drop a fileset. */
    DROP_FILESET(1L << 12),
    /** The privilege to read a fileset. */
    READ_FILESET(1L << 13),
    /** The privilege to write a fileset. */
    WRITE_FILESET(1L << 14),
    /** The privilege to create a topic. */
    CREATE_TOPIC(1L << 15),
    /** The privilege to drop a topic. */
    DROP_TOPIC(1L << 16),
    /** The privilege to read a topic. */
    READ_TOPIC(1L << 17),
    /** The privilege to write a topic. */
    WRITE_TOPIC(1L << 18),
    /** The privilege to use a metalake, the user can load the information of the metalake. */
    USE_METALAKE(1L << 19),
    /** The privilege to manage a metalake, including drop and alter a metalake. */
    MANAGE_METALAKE(1L << 20),
    /** The privilege to create a metalake. */
    CREATE_METALAKE(1L << 21),
    /** The privilege to manage users, including add,remove and get a user */
    MANAGE_USER(1L << 22),
    /** The privilege to manage groups, including add,remove and get a group. */
    MANAGE_GROUP(1L << 23),
    /** The privilege to manage roles, including create,drop,alter,grant and revoke a role. */
    MANAGE_ROLE(1L << 24);

    private final long value;

    Name(long value) {
      this.value = value;
    }

    /**
     * Return the value of Name
     *
     * @return The value of Name
     */
    public long getValue() {
      return value;
    }
  }
}
