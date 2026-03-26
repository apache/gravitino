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

package org.apache.gravitino.audit.v2;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.audit.AuditLog.Operation;
import org.apache.gravitino.audit.AuditLog.Status;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;

public class CompatibilityUtils {

  private static final ImmutableMap<OperationType, Operation> operationTypeMap =
      ImmutableMap.<OperationType, Operation>builder()
          .put(OperationType.CREATE_METALAKE, Operation.CREATE_METALAKE)
          .put(OperationType.ALTER_METALAKE, Operation.ALTER_METALAKE)
          .put(OperationType.DROP_METALAKE, Operation.DROP_METALAKE)
          .put(OperationType.LOAD_METALAKE, Operation.LOAD_METALAKE)
          .put(OperationType.LIST_METALAKE, Operation.LIST_METALAKE)
          .put(OperationType.ENABLE_METALAKE, Operation.ENABLE_METALAKE)
          .put(OperationType.DISABLE_METALAKE, Operation.DISABLE_METALAKE)
          .put(OperationType.CREATE_CATALOG, Operation.CREATE_CATALOG)
          .put(OperationType.DROP_CATALOG, Operation.DROP_CATALOG)
          .put(OperationType.ALTER_CATALOG, Operation.ALTER_CATALOG)
          .put(OperationType.LOAD_CATALOG, Operation.LOAD_CATALOG)
          .put(OperationType.LIST_CATALOG, Operation.LIST_CATALOG)
          .put(OperationType.ENABLE_CATALOG, Operation.ENABLE_CATALOG)
          .put(OperationType.DISABLE_CATALOG, Operation.DISABLE_CATALOG)
          .put(OperationType.CREATE_SCHEMA, Operation.CREATE_SCHEMA)
          .put(OperationType.DROP_SCHEMA, Operation.DROP_SCHEMA)
          .put(OperationType.ALTER_SCHEMA, Operation.ALTER_SCHEMA)
          .put(OperationType.LOAD_SCHEMA, Operation.LOAD_SCHEMA)
          .put(OperationType.LIST_SCHEMA, Operation.LIST_SCHEMA)
          .put(OperationType.SCHEMA_EXISTS, Operation.SCHEMA_EXISTS)
          .put(OperationType.CREATE_TABLE, Operation.CREATE_TABLE)
          .put(OperationType.DROP_TABLE, Operation.DROP_TABLE)
          .put(OperationType.PURGE_TABLE, Operation.PURGE_TABLE)
          .put(OperationType.LOAD_TABLE, Operation.LOAD_TABLE)
          .put(OperationType.LOAD_TABLE_CREDENTIAL, Operation.LOAD_TABLE_CREDENTIAL)
          .put(OperationType.PLAN_TABLE_SCAN, Operation.PLAN_TABLE_SCAN)
          .put(OperationType.LIST_TABLE, Operation.LIST_TABLE)
          .put(OperationType.ALTER_TABLE, Operation.ALTER_TABLE)
          .put(OperationType.RENAME_TABLE, Operation.RENAME_TABLE)
          .put(OperationType.REGISTER_TABLE, Operation.REGISTER_TABLE)
          .put(OperationType.TABLE_EXISTS, Operation.TABLE_EXISTS)
          .put(OperationType.CREATE_TAG, Operation.CREATE_TAG)
          .put(OperationType.GET_TAG, Operation.GET_TAG)
          .put(OperationType.GET_TAG_FOR_METADATA_OBJECT, Operation.GET_TAG_FOR_METADATA_OBJECT)
          .put(OperationType.DELETE_TAG, Operation.DELETE_TAG)
          .put(OperationType.ALTER_TAG, Operation.ALTER_TAG)
          .put(OperationType.LIST_TAG, Operation.LIST_TAG)
          .put(
              OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT,
              Operation.ASSOCIATE_TAGS_FOR_METADATA_OBJECT)
          .put(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, Operation.LIST_TAGS_FOR_METADATA_OBJECT)
          .put(
              OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT,
              Operation.LIST_TAGS_INFO_FOR_METADATA_OBJECT)
          .put(OperationType.LIST_METADATA_OBJECTS_FOR_TAG, Operation.LIST_METADATA_OBJECTS_FOR_TAG)
          .put(OperationType.LIST_TAGS_INFO, Operation.LIST_TAGS_INFO)
          .put(OperationType.DROP_FILESET, Operation.DROP_FILESET)
          .put(OperationType.ALTER_FILESET, Operation.ALTER_FILESET)
          .put(OperationType.CREATE_FILESET, Operation.CREATE_FILESET)
          .put(OperationType.LIST_FILESET, Operation.LIST_FILESET)
          .put(OperationType.LIST_FILESET_FILES, Operation.LIST_FILESET_FILES)
          .put(OperationType.LOAD_FILESET, Operation.LOAD_FILESET)
          .put(OperationType.GET_FILESET_LOCATION, Operation.GET_FILE_LOCATION)
          .put(OperationType.ADD_PARTITION, Operation.ADD_PARTITION)
          .put(OperationType.DROP_PARTITION, Operation.DROP_PARTITION)
          .put(OperationType.PURGE_PARTITION, Operation.PURGE_PARTITION)
          .put(OperationType.PARTITION_EXISTS, Operation.PARTITION_EXIST)
          .put(OperationType.LOAD_PARTITION, Operation.GET_PARTITION)
          .put(OperationType.LIST_PARTITION, Operation.LIST_PARTITION)
          .put(OperationType.LIST_PARTITION_NAMES, Operation.LIST_PARTITION)
          .put(OperationType.CREATE_TOPIC, Operation.CREATE_TOPIC)
          .put(OperationType.ALTER_TOPIC, Operation.ALTER_TOPIC)
          .put(OperationType.DROP_TOPIC, Operation.DROP_TOPIC)
          .put(OperationType.LIST_TOPIC, Operation.LIST_TOPIC)
          .put(OperationType.LOAD_TOPIC, Operation.LOAD_TOPIC)
          .put(OperationType.CREATE_VIEW, Operation.CREATE_VIEW)
          .put(OperationType.ALTER_VIEW, Operation.ALTER_VIEW)
          .put(OperationType.DROP_VIEW, Operation.DROP_VIEW)
          .put(OperationType.LOAD_VIEW, Operation.LOAD_VIEW)
          .put(OperationType.VIEW_EXISTS, Operation.VIEW_EXISTS)
          .put(OperationType.RENAME_VIEW, Operation.RENAME_VIEW)
          .put(OperationType.LIST_VIEW, Operation.LIST_VIEW)
          .put(OperationType.REGISTER_MODEL, Operation.REGISTER_MODEL)
          .put(OperationType.DELETE_MODEL, Operation.DELETE_MODEL)
          .put(OperationType.GET_MODEL, Operation.GET_MODEL)
          .put(OperationType.LIST_MODEL, Operation.LIST_MODEL)
          .put(OperationType.ALTER_MODEL, Operation.ALTER_MODEL)
          .put(OperationType.LINK_MODEL_VERSION, Operation.LINK_MODEL_VERSION)
          .put(OperationType.DELETE_MODEL_VERSION, Operation.DELETE_MODEL_VERSION)
          .put(OperationType.GET_MODEL_VERSION, Operation.GET_MODEL_VERSION)
          .put(OperationType.GET_MODEL_VERSION_URI, Operation.GET_MODEL_VERSION_URI)
          .put(OperationType.LIST_MODEL_VERSIONS, Operation.LIST_MODEL_VERSIONS)
          .put(OperationType.LIST_MODEL_VERSION_INFOS, Operation.LIST_MODEL_VERSION_INFOS)
          .put(
              OperationType.REGISTER_AND_LINK_MODEL_VERSION,
              Operation.REGISTER_AND_LINK_MODEL_VERSION)
          .put(OperationType.ALTER_MODEL_VERSION, Operation.ALTER_MODEL_VERSION)
          .put(OperationType.ADD_USER, Operation.ADD_USER)
          .put(OperationType.REMOVE_USER, Operation.REMOVE_USER)
          .put(OperationType.GET_USER, Operation.GET_USER)
          .put(OperationType.LIST_USERS, Operation.LIST_USERS)
          .put(OperationType.LIST_USER_NAMES, Operation.LIST_USER_NAMES)
          .put(OperationType.GRANT_USER_ROLES, Operation.GRANT_USER_ROLES)
          .put(OperationType.REVOKE_USER_ROLES, Operation.REVOKE_USER_ROLES)
          .put(OperationType.ADD_GROUP, Operation.ADD_GROUP)
          .put(OperationType.REMOVE_GROUP, Operation.REMOVE_GROUP)
          .put(OperationType.GET_GROUP, Operation.GET_GROUP)
          .put(OperationType.LIST_GROUPS, Operation.LIST_GROUPS)
          .put(OperationType.LIST_GROUP_NAMES, Operation.LIST_GROUP_NAMES)
          .put(OperationType.GRANT_GROUP_ROLES, Operation.GRANT_GROUP_ROLES)
          .put(OperationType.REVOKE_GROUP_ROLES, Operation.REVOKE_GROUP_ROLES)
          .put(OperationType.CREATE_ROLE, Operation.CREATE_ROLE)
          .put(OperationType.DELETE_ROLE, Operation.DELETE_ROLE)
          .put(OperationType.GET_ROLE, Operation.GET_ROLE)
          .put(OperationType.LIST_ROLE_NAMES, Operation.LIST_ROLE_NAMES)
          .put(OperationType.GRANT_PRIVILEGES, Operation.GRANT_PRIVILEGES)
          .put(OperationType.REVOKE_PRIVILEGES, Operation.REVOKE_PRIVILEGES)
          .put(OperationType.OVERRIDE_PRIVILEGES, Operation.OVERRIDE_PRIVILEGES)
          .put(OperationType.GET_OWNER, Operation.GET_OWNER)
          .put(OperationType.SET_OWNER, Operation.SET_OWNER)
          .put(OperationType.LIST_JOB_TEMPLATES, Operation.LIST_JOB_TEMPLATES)
          .put(OperationType.REGISTER_JOB_TEMPLATE, Operation.REGISTER_JOB_TEMPLATE)
          .put(OperationType.GET_JOB_TEMPLATE, Operation.GET_JOB_TEMPLATE)
          .put(OperationType.ALTER_JOB_TEMPLATE, Operation.ALTER_JOB_TEMPLATE)
          .put(OperationType.DELETE_JOB_TEMPLATE, Operation.DELETE_JOB_TEMPLATE)
          .put(OperationType.LIST_JOBS, Operation.LIST_JOBS)
          .put(OperationType.RUN_JOB, Operation.RUN_JOB)
          .put(OperationType.GET_JOB, Operation.GET_JOB)
          .put(OperationType.CANCEL_JOB, Operation.CANCEL_JOB)
          .put(OperationType.LIST_STATISTICS, Operation.LIST_STATISTICS)
          .put(OperationType.LIST_PARTITION_STATISTICS, Operation.LIST_PARTITION_STATISTICS)
          .put(OperationType.DROP_STATISTICS, Operation.DROP_STATISTICS)
          .put(OperationType.DROP_PARTITION_STATISTICS, Operation.DROP_PARTITION_STATISTICS)
          .put(OperationType.UPDATE_STATISTICS, Operation.UPDATE_STATISTICS)
          .put(OperationType.UPDATE_PARTITION_STATISTICS, Operation.UPDATE_PARTITION_STATISTICS)
          .put(OperationType.CREATE_POLICY, Operation.CREATE_POLICY)
          .put(OperationType.GET_POLICY, Operation.GET_POLICY)
          .put(OperationType.ALTER_POLICY, Operation.ALTER_POLICY)
          .put(OperationType.DELETE_POLICY, Operation.DELETE_POLICY)
          .put(OperationType.ENABLE_POLICY, Operation.ENABLE_POLICY)
          .put(OperationType.DISABLE_POLICY, Operation.DISABLE_POLICY)
          .put(OperationType.LIST_POLICY, Operation.LIST_POLICY)
          .put(OperationType.LIST_POLICY_INFO, Operation.LIST_POLICY_INFO)
          .put(
              OperationType.LIST_METADATA_OBJECTS_FOR_POLICY,
              Operation.LIST_METADATA_OBJECTS_FOR_POLICY)
          .put(
              OperationType.LIST_POLICY_INFOS_FOR_METADATA_OBJECT,
              Operation.LIST_POLICY_INFOS_FOR_METADATA_OBJECT)
          .put(
              OperationType.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT,
              Operation.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT)
          .put(
              OperationType.GET_POLICY_FOR_METADATA_OBJECT,
              Operation.GET_POLICY_FOR_METADATA_OBJECT)
          .put(OperationType.UNKNOWN, Operation.UNKNOWN_OPERATION)
          .build();

  static Operation toAuditLogOperation(OperationType operationType) {
    if (operationType == null) {
      return Operation.UNKNOWN_OPERATION;
    }

    return operationTypeMap.getOrDefault(operationType, Operation.UNKNOWN_OPERATION);
  }

  static Status toAuditLogStatus(OperationStatus operationStatus) {
    if (operationStatus == OperationStatus.SUCCESS) {
      return Status.SUCCESS;
    } else if (operationStatus == OperationStatus.FAILURE) {
      return Status.FAILURE;
    } else {
      return Status.UNKNOWN;
    }
  }
}
