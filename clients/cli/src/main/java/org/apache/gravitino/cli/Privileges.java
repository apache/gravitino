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

package org.apache.gravitino.cli;

import java.util.HashSet;
import org.apache.gravitino.authorization.Privilege;

/**
 * This class contains the list of valid privileges and methods to check if a given privilege is
 * valid.
 */
public class Privileges {
  /** Represents the privilege to create a catalog. */
  public static final String CREATE_CATALOG = "create_catalog";
  /** Represents the privilege to use a catalog. */
  public static final String USE_CATALOG = "use_catalog";
  /** Represents the privilege to create a schema. */
  public static final String CREATE_SCHEMA = "create_schema";
  /** Represents the privilege to use a schema. */
  public static final String USE_SCHEMA = "use_schema";
  /** Represents the privilege to create a table. */
  public static final String CREATE_TABLE = "create_table";
  /** Represents the privilege to modify a table. */
  public static final String MODIFY_TABLE = "modify_table";
  /** Represents the privilege to select from a table. */
  public static final String SELECT_TABLE = "select_table";
  /** Represents the privilege to create a fileset. */
  public static final String CREATE_FILESET = "create_fileset";
  /** Represents the privilege to write to a fileset. */
  public static final String WRITE_FILESET = "write_fileset";
  /** Represents the privilege to read from a fileset. */
  public static final String READ_FILESET = "read_fileset";
  /** Represents the privilege to create a topic. */
  public static final String CREATE_TOPIC = "create_topic";
  /** Represents the privilege to produce to a topic. */
  public static final String PRODUCE_TOPIC = "produce_topic";
  /** Represents the privilege to consume from a topic. */
  public static final String CONSUME_TOPIC = "consume_topic";
  /** Represents the privilege to manage users. */
  public static final String MANAGE_USERS = "manage_users";
  /** Represents the privilege to create a role. */
  public static final String CREATE_ROLE = "create_role";
  /** Represents the privilege to manage grants. */
  public static final String MANAGE_GRANTS = "manage_grants";

  private static final HashSet<String> VALID_PRIVILEGES = new HashSet<>();

  static {
    VALID_PRIVILEGES.add(CREATE_CATALOG);
    VALID_PRIVILEGES.add(USE_CATALOG);
    VALID_PRIVILEGES.add(CREATE_SCHEMA);
    VALID_PRIVILEGES.add(USE_SCHEMA);
    VALID_PRIVILEGES.add(CREATE_TABLE);
    VALID_PRIVILEGES.add(MODIFY_TABLE);
    VALID_PRIVILEGES.add(SELECT_TABLE);
    VALID_PRIVILEGES.add(CREATE_FILESET);
    VALID_PRIVILEGES.add(WRITE_FILESET);
    VALID_PRIVILEGES.add(READ_FILESET);
    VALID_PRIVILEGES.add(CREATE_TOPIC);
    VALID_PRIVILEGES.add(PRODUCE_TOPIC);
    VALID_PRIVILEGES.add(CONSUME_TOPIC);
    VALID_PRIVILEGES.add(MANAGE_USERS);
    VALID_PRIVILEGES.add(CREATE_ROLE);
    VALID_PRIVILEGES.add(MANAGE_GRANTS);
  }

  /**
   * Checks if a given privilege is a valid one.
   *
   * @param privilege The privilege to check.
   * @return true if the privilege is valid, false otherwise.
   */
  public static boolean isValid(String privilege) {
    return VALID_PRIVILEGES.contains(privilege);
  }

  /**
   * Converts a string representation of a privilege to the corresponding {@link Privilege.Name}.
   *
   * @param privilege the privilege to be converted.
   * @return the corresponding {@link Privilege.Name} constant, or nullif the privilege is unknown.
   */
  public static Privilege.Name toName(String privilege) {
    switch (privilege) {
      case CREATE_CATALOG:
        return Privilege.Name.CREATE_CATALOG;
      case USE_CATALOG:
        return Privilege.Name.USE_CATALOG;
      case CREATE_SCHEMA:
        return Privilege.Name.CREATE_SCHEMA;
      case USE_SCHEMA:
        return Privilege.Name.USE_SCHEMA;
      case CREATE_TABLE:
        return Privilege.Name.CREATE_TABLE;
      case MODIFY_TABLE:
        return Privilege.Name.MODIFY_TABLE;
      case SELECT_TABLE:
        return Privilege.Name.SELECT_TABLE;
      case CREATE_FILESET:
        return Privilege.Name.CREATE_FILESET;
      case WRITE_FILESET:
        return Privilege.Name.WRITE_FILESET;
      case READ_FILESET:
        return Privilege.Name.READ_FILESET;
      case CREATE_TOPIC:
        return Privilege.Name.CREATE_TOPIC;
      case PRODUCE_TOPIC:
        return Privilege.Name.PRODUCE_TOPIC;
      case CONSUME_TOPIC:
        return Privilege.Name.CONSUME_TOPIC;
      case MANAGE_USERS:
        return Privilege.Name.MANAGE_USERS;
      case CREATE_ROLE:
        return Privilege.Name.CREATE_ROLE;
      case MANAGE_GRANTS:
        return Privilege.Name.MANAGE_GRANTS;
      default:
        System.err.println(ErrorMessages.UNKNOWN_PRIVILEGE + " " + privilege);
        return null;
    }
  }
}
