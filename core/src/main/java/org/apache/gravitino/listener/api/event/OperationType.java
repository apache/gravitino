/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.listener.api.event;

public enum OperationType {
  // Table operations
  CREATE_TABLE,
  DROP_TABLE,
  PURGE_TABLE,
  LOAD_TABLE,
  LIST_TABLE,
  ALTER_TABLE,
  RENAME_TABLE,
  REGISTER_TABLE,
  TABLE_EXISTS,

  // Tag operations
  CREATE_TAG,
  GET_TAG,
  GET_TAG_FOR_METADATA_OBJECT,
  DELETE_TAG,
  ALTER_TAG,
  LIST_TAG,
  ASSOCIATE_TAGS_FOR_METADATA_OBJECT,
  LIST_TAGS_FOR_METADATA_OBJECT,
  LIST_TAGS_INFO_FOR_METADATA_OBJECT,
  LIST_METADATA_OBJECTS_FOR_TAG,
  LIST_TAGS_INFO,

  // Schema operations
  CREATE_SCHEMA,
  DROP_SCHEMA,
  ALTER_SCHEMA,
  LOAD_SCHEMA,
  LIST_SCHEMA,
  SCHEMA_EXISTS,

  // Fileset operations
  DROP_FILESET,
  ALTER_FILESET,
  CREATE_FILESET,
  LIST_FILESET,
  LOAD_FILESET,
  GET_FILESET_LOCATION,

  // Catalog operations
  CREATE_CATALOG,
  DROP_CATALOG,
  ALTER_CATALOG,
  LOAD_CATALOG,
  LIST_CATALOG,

  // Partition event
  ADD_PARTITION,
  DROP_PARTITION,
  PURGE_PARTITION,
  PARTITION_EXISTS,
  LOAD_PARTITION,
  LIST_PARTITION,
  LIST_PARTITION_NAMES,

  // Topic event
  CREATE_TOPIC,
  ALTER_TOPIC,
  DROP_TOPIC,
  LIST_TOPIC,
  LOAD_TOPIC,

  // Metalake event
  CREATE_METALAKE,
  ALTER_METALAKE,
  LIST_METALAKE,
  DROP_METALAKE,
  LOAD_METALAKE,

  // View event
  CREATE_VIEW,
  ALTER_VIEW,
  DROP_VIEW,
  LOAD_VIEW,
  VIEW_EXISTS,
  RENAME_VIEW,
  LIST_VIEW,

  // Model event
  REGISTER_MODEL,
  DELETE_MODEL,
  GET_MODEL,
  LIST_MODEL,

  // Model Version
  LINK_MODEL_VERSION,
  DELETE_MODEL_VERSION,
  GET_MODEL_VERSION,
  LIST_MODEL_VERSIONS,
  REGISTER_AND_LINK_MODEL_VERSION,

  // User
  ADD_USER,
  REMOVE_USER,
  GET_USER,
  LIST_USERS,
  LIST_USER_NAMES,

  // Group
  ADD_GROUP,
  REMOVE_GROUP,
  GET_GROUP,
  LIST_GROUPS,
  LIST_GROUP_NAMES,

  // TODO ROLE

  UNKNOWN,
}
