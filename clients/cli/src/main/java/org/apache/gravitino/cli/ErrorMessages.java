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

/** User friendly error messages. */
public class ErrorMessages {

  /** Thrown when a catalog already exists. */
  public static final String CATALOG_EXISTS = "Catalog already exists.";

  /** Cannot delete the anonymous user, as it may cause unexpected behavior. */
  public static final String DELETE_ANONYMOUS_USER =
      "Can't delete anonymous user. This will cause unexpected behavior.";

  /** Thrown when a fileset already exists. */
  public static final String FILESET_EXISTS = "Fileset already exists.";

  /** Thrown when a group already exists. */
  public static final String GROUP_EXISTS = "Group already exists.";

  /** Thrown when a metalake already exists. */
  public static final String METALAKE_EXISTS = "Metalake already exists.";

  /** Thrown when a model already exists. */
  public static final String MODEL_EXISTS = "Model already exists.";

  /** Thrown when a role already exists. */
  public static final String ROLE_EXISTS = "Role already exists.";

  /** Thrown when a schema already exists. */
  public static final String SCHEMA_EXISTS = "Schema already exists.";

  /** Thrown when a table already exists. */
  public static final String TABLE_EXISTS = "Table already exists.";

  /** Thrown when a tag already exists. */
  public static final String TAG_EXISTS = "Tag already exists.";

  /** Thrown when a topic already exists. */
  public static final String TOPIC_EXISTS = "Topic already exists.";

  /** Thrown when a user already exists. */
  public static final String USER_EXISTS = "User already exists.";

  /** Indicates that the entity is in use and must be disabled before removal. */
  public static final String ENTITY_IN_USE = " in use, please disable it first.";

  /** Error when both --enable and --disable are used together. */
  public static final String INVALID_ENABLE_DISABLE =
      "Unable to use --enable and --disable at the same time.";

  /** Error when both --user and --group are specified together. */
  public static final String INVALID_OWNER_COMMAND =
      "Unsupported combination of options either use --user or --group.";

  /** Error when both --name and --property are specified together. */
  public static final String INVALID_REMOVE_COMMAND =
      "Unsupported combination of options either use --name or --property.";

  /** Error when invalid option combinations are used for set commands. */
  public static final String INVALID_SET_COMMAND =
      "Unsupported combination of options either use --name, --user, --group or --property and --value.";

  /** Thrown when help message cannot be loaded. */
  public static final String HELP_FAILED = "Failed to load help message: ";

  /** Indicates that the entity name format is invalid. */
  public static final String MALFORMED_NAME = "Malformed entity name.";

  /** Indicates that the --columnfile option is missing. */
  public static final String MISSING_COLUMN_FILE = "Missing --columnfile option.";

  /** Indicates that both --comment and --rename options are missing. */
  public static final String MISSING_COMMENT_AND_RENAME = "Missing --comment and --rename options.";

  /** Indicates that the --datatype option is missing. */
  public static final String MISSING_DATATYPE = "Missing --datatype option.";

  /** Indicates that required entity names are missing. */
  public static final String MISSING_ENTITIES = "Missing required entity names: ";

  /** Indicates that the --group option is missing. */
  public static final String MISSING_GROUP = "Missing --group option.";

  /** Indicates that the --metalake option is missing. */
  public static final String MISSING_METALAKE = "Missing --metalake option.";

  /** Indicates that the --name option is missing. */
  public static final String MISSING_NAME = "Missing --name option.";

  /** Indicates that the --privilege option is missing. */
  public static final String MISSING_PRIVILEGES = "Missing --privilege option.";

  /** Indicates that the --property option is missing. */
  public static final String MISSING_PROPERTY = "Missing --property option.";

  /** Indicates that both --property and --value options are missing. */
  public static final String MISSING_PROPERTY_AND_VALUE = "Missing --property and --value options.";

  /** Indicates that the --role option is missing. */
  public static final String MISSING_ROLE = "Missing --role option.";

  /** Indicates that the --tag option is missing. */
  public static final String MISSING_TAG = "Missing --tag option.";

  /** Indicates that the --uris option is missing. */
  public static final String MISSING_URIS = "Missing --uris option.";

  /** Indicates that the --user option is missing. */
  public static final String MISSING_USER = "Missing --user option.";

  /** Indicates that the --value option is missing. */
  public static final String MISSING_VALUE = "Missing --value option.";

  /** Error when multiple --alias options are specified. */
  public static final String MULTIPLE_ALIASES_COMMAND_ERROR =
      "This command only supports one --alias option.";

  /** Error when multiple --role options are specified. */
  public static final String MULTIPLE_ROLE_COMMAND_ERROR =
      "This command only supports one --role option.";

  /** Error when multiple --tag options are specified. */
  public static final String MULTIPLE_TAG_COMMAND_ERROR =
      "This command only supports one --tag option.";

  /** Indicates that the --provider option is missing. */
  public static final String MISSING_PROVIDER = "Missing --provider option.";

  /** Thrown when model registration fails. */
  public static final String REGISTER_FAILED = "Failed to register model: ";

  /** Thrown when an unknown catalog name is used. */
  public static final String UNKNOWN_CATALOG = "Unknown catalog name.";

  /** Thrown when an unknown column name is used. */
  public static final String UNKNOWN_COLUMN = "Unknown column name.";

  /** Thrown when an unknown entity is referenced. */
  public static final String UNKNOWN_ENTITY = "Unknown entity.";

  /** Thrown when an unknown fileset name is used. */
  public static final String UNKNOWN_FILESET = "Unknown fileset name.";

  /** Thrown when an unknown group is used. */
  public static final String UNKNOWN_GROUP = "Unknown group.";

  /** Thrown when an unknown metalake name is used. */
  public static final String UNKNOWN_METALAKE = "Unknown metalake name.";

  /** Thrown when an unknown model name is used. */
  public static final String UNKNOWN_MODEL = "Unknown model name.";

  /** Thrown when an unknown model version is used. */
  public static final String UNKNOWN_MODEL_VERSION = "Unknown model version.";

  /** Thrown when an unknown privilege is used. */
  public static final String UNKNOWN_PRIVILEGE = "Unknown privilege";

  /** Thrown when an unknown role is used. */
  public static final String UNKNOWN_ROLE = "Unknown role.";

  /** Thrown when an unknown schema name is used. */
  public static final String UNKNOWN_SCHEMA = "Unknown schema name.";

  /** Thrown when an unknown table name is used. */
  public static final String UNKNOWN_TABLE = "Unknown table name.";

  /** Thrown when an unknown tag is used. */
  public static final String UNKNOWN_TAG = "Unknown tag.";

  /** Thrown when an unknown topic name is used. */
  public static final String UNKNOWN_TOPIC = "Unknown topic name.";

  /** Thrown when an unknown user is used. */
  public static final String UNKNOWN_USER = "Unknown user.";

  /** Error while parsing the command line arguments. */
  public static final String PARSE_ERROR = "Error parsing command line: ";

  /** Indicates that too many arguments were provided. */
  public static final String TOO_MANY_ARGUMENTS = "Too many arguments.";

  /** Indicates that the entity does not support the requested action. */
  public static final String UNSUPPORTED_ACTION = "Entity doesn't support this action.";

  /** Indicates that the command is unsupported or unknown. */
  public static final String UNSUPPORTED_COMMAND = "Unsupported or unknown command.";
}
