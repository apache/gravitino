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

/* User friendly error messages. */
public class ErrorMessages {
  public static final String UNSUPPORTED_COMMAND = "Unsupported or unknown command.";
  public static final String UNKNOWN_ENTITY = "Unknown entity.";
  public static final String TOO_MANY_ARGUMENTS = "Too many arguments.";
  public static final String UNKNOWN_METALAKE = "Unknown metalake name.";
  public static final String UNKNOWN_CATALOG = "Unknown catalog name.";
  public static final String UNKNOWN_SCHEMA = "Unknown schema name.";
  public static final String UNKNOWN_TABLE = "Unknown table name.";
  public static final String MALFORMED_NAME = "Malformed entity name.";
  public static final String MISSING_NAME = "Missing --name option.";
  public static final String METALAKE_EXISTS = "Metalake already exists.";
  public static final String CATALOG_EXISTS = "Catalog already exists.";
  public static final String SCHEMA_EXISTS = "Schema already exists.";
  public static final String UNKNOWN_USER = "Unknown user.";
  public static final String USER_EXISTS = "User already exists.";
  public static final String UNKNOWN_GROUP = "Unknown group.";
  public static final String GROUP_EXISTS = "Group already exists.";
  public static final String UNKNOWN_TAG = "Unknown tag.";
  public static final String MULTIPLE_TAG_COMMAND_ERROR =
      "Error: The current command only supports one --tag option.";
  public static final String TAG_EXISTS = "Tag already exists.";
  public static final String UNKNOWN_COLUMN = "Unknown column.";
  public static final String COLUMN_EXISTS = "Column already exists.";
  public static final String UNKNOWN_TOPIC = "Unknown topic.";
  public static final String TOPIC_EXISTS = "Topic already exists.";
  public static final String UNKNOWN_FILESET = "Unknown fileset.";
  public static final String FILESET_EXISTS = "Fileset already exists.";
  public static final String TAG_EMPTY = "Error: Must configure --tag option.";
  public static final String UNKNOWN_ROLE = "Unknown role.";
  public static final String ROLE_EXISTS = "Role already exists.";
  public static final String TABLE_EXISTS = "Table already exists.";
  public static final String INVALID_SET_COMMAND =
      "Unsupported combination of options either use --name, --user, --group or --property and --value.";
  public static final String INVALID_REMOVE_COMMAND =
      "Unsupported combination of options either use --name or --property.";
  public static final String INVALID_OWNER_COMMAND =
      "Unsupported combination of options either use --user or --group.";
  public static final String UNSUPPORTED_ACTION = "Entity doesn't support this action.";
}
