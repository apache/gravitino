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
package org.apache.gravitino.authorization;

import org.apache.gravitino.annotation.Unstable;

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

  /**
   * @return The condition of the privilege. `ALLOW` means that you are allowed to use the
   *     privilege, `DENY` means that you are denied to use the privilege
   */
  Condition condition();

  /** The name of this privilege. */
  enum Name {
    /** The privilege to create a catalog. */
    CREATE_CATALOG(0L, 1L),
    /** The privilege to drop a catalog. */
    DROP_CATALOG(0L, 1L << 1),
    /** The privilege to alter a catalog. */
    ALTER_CATALOG(0L, 1L << 2),
    /** The privilege to use a catalog. */
    USE_CATALOG(0L, 1L << 3),
    /** The privilege to create a schema. */
    CREATE_SCHEMA(0L, 1L << 4),
    /** The privilege to drop a schema. */
    DROP_SCHEMA(0L, 1L << 5),
    /** The privilege to alter a schema. */
    ALTER_SCHEMA(0L, 1L << 6),
    /** the privilege to use a schema. */
    USE_SCHEMA(0L, 1L << 7),
    /** The privilege to create a table. */
    CREATE_TABLE(0L, 1L << 8),
    /** The privilege to drop a table. */
    DROP_TABLE(0L, 1L << 9),
    /** The privilege to write a table. */
    WRITE_TABLE(0L, 1L << 10),
    /** The privilege to read a table. */
    READ_TABLE(0L, 1L << 11),
    /** The privilege to create a fileset. */
    CREATE_FILESET(0L, 1L << 12),
    /** The privilege to drop a fileset. */
    DROP_FILESET(0L, 1L << 13),
    /** The privilege to write a fileset. */
    WRITE_FILESET(0L, 1L << 14),
    /** The privilege to read a fileset. */
    READ_FILESET(0L, 1L << 15),
    /** The privilege to create a topic. */
    CREATE_TOPIC(0L, 1L << 16),
    /** The privilege to drop a topic. */
    DROP_TOPIC(0L, 1L << 17),
    /** The privilege to write a topic. */
    WRITE_TOPIC(0L, 1L << 18),
    /** The privilege to read a topic. */
    READ_TOPIC(0L, 1L << 19),
    /** The privilege to add a user */
    ADD_USER(0L, 1L << 20),
    /** The privilege to remove a user */
    REMOVE_USER(0L, 1L << 21),
    /** The privilege to get a user */
    GET_USER(0L, 1L << 22),
    /** The privilege to add a group */
    ADD_GROUP(0L, 1L << 23),
    /** The privilege to remove a group */
    REMOVE_GROUP(0L, 1L << 24),
    /** The privilege to get a group */
    GET_GROUP(0L, 1L << 25),
    /** The privilege to create a role */
    CREATE_ROLE(0L, 1L << 26),
    /** The privilege to delete a role */
    DELETE_ROLE(0L, 1L << 27),
    /** The privilege to grant a role to the user or the group. */
    GRANT_ROLE(0L, 1L << 28),
    /** The privilege to revoke a role from the user or the group. */
    REVOKE_ROLE(0L, 1L << 29),
    /** The privilege to get a role */
    GET_ROLE(0L, 1L << 30);

    private final long highBits;
    private final long lowBits;

    Name(long highBits, long lowBits) {
      this.highBits = highBits;
      this.lowBits = lowBits;
    }

    /**
     * Return the low bits of Name
     *
     * @return The low bits of Name
     */
    public long getLowBits() {
      return lowBits;
    }

    /**
     * Return the high bits of Name
     *
     * @return The high bits of Name
     */
    public long getHighBits() {
      return highBits;
    }
  }

  /**
   * The condition of this privilege. `ALLOW` means that you are allowed to use the privilege,
   * `DENY` means that you are denied to use the privilege. If you have `ALLOW` and `DENY` for the
   * same privilege name of the same securable object, the `DENY` will take effect.
   */
  enum Condition {
    /** Allow to use the privilege */
    ALLOW,
    /** Deny to use the privilege */
    DENY
  }
}
