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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.EnumSet;
import org.apache.gravitino.audit.AuditLog.Operation;
import org.apache.gravitino.audit.AuditLog.Status;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCompatibilityUtils {

  @Test
  public void testToAuditLogOperation() {
    Object[][] testCases = {
      {OperationType.CREATE_METALAKE, Operation.CREATE_METALAKE},
      {OperationType.ALTER_METALAKE, Operation.ALTER_METALAKE},
      {OperationType.DROP_METALAKE, Operation.DROP_METALAKE},
      {OperationType.LOAD_METALAKE, Operation.LOAD_METALAKE},
      {OperationType.LIST_METALAKE, Operation.LIST_METALAKE},
      {OperationType.ENABLE_METALAKE, Operation.ENABLE_METALAKE},
      {OperationType.DISABLE_METALAKE, Operation.DISABLE_METALAKE},
      {OperationType.CREATE_CATALOG, Operation.CREATE_CATALOG},
      {OperationType.ALTER_CATALOG, Operation.ALTER_CATALOG},
      {OperationType.DROP_CATALOG, Operation.DROP_CATALOG},
      {OperationType.LOAD_CATALOG, Operation.LOAD_CATALOG},
      {OperationType.LIST_CATALOG, Operation.LIST_CATALOG},
      {OperationType.ENABLE_CATALOG, Operation.ENABLE_CATALOG},
      {OperationType.DISABLE_CATALOG, Operation.DISABLE_CATALOG},
      {OperationType.CREATE_SCHEMA, Operation.CREATE_SCHEMA},
      {OperationType.ALTER_SCHEMA, Operation.ALTER_SCHEMA},
      {OperationType.DROP_SCHEMA, Operation.DROP_SCHEMA},
      {OperationType.LOAD_SCHEMA, Operation.LOAD_SCHEMA},
      {OperationType.LIST_SCHEMA, Operation.LIST_SCHEMA},
      {OperationType.SCHEMA_EXISTS, Operation.SCHEMA_EXISTS},
      {OperationType.CREATE_TABLE, Operation.CREATE_TABLE},
      {OperationType.ALTER_TABLE, Operation.ALTER_TABLE},
      {OperationType.DROP_TABLE, Operation.DROP_TABLE},
      {OperationType.PURGE_TABLE, Operation.PURGE_TABLE},
      {OperationType.LOAD_TABLE, Operation.LOAD_TABLE},
      {OperationType.LOAD_TABLE_CREDENTIAL, Operation.LOAD_TABLE_CREDENTIAL},
      {OperationType.PLAN_TABLE_SCAN, Operation.PLAN_TABLE_SCAN},
      {OperationType.TABLE_EXISTS, Operation.TABLE_EXISTS},
      {OperationType.LIST_TABLE, Operation.LIST_TABLE},
      {OperationType.RENAME_TABLE, Operation.RENAME_TABLE},
      {OperationType.REGISTER_TABLE, Operation.REGISTER_TABLE},
      {OperationType.CREATE_TAG, Operation.CREATE_TAG},
      {OperationType.GET_TAG, Operation.GET_TAG},
      {OperationType.GET_TAG_FOR_METADATA_OBJECT, Operation.GET_TAG_FOR_METADATA_OBJECT},
      {OperationType.DELETE_TAG, Operation.DELETE_TAG},
      {OperationType.ALTER_TAG, Operation.ALTER_TAG},
      {OperationType.LIST_TAG, Operation.LIST_TAG},
      {
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT,
        Operation.ASSOCIATE_TAGS_FOR_METADATA_OBJECT
      },
      {OperationType.LIST_TAGS_FOR_METADATA_OBJECT, Operation.LIST_TAGS_FOR_METADATA_OBJECT},
      {
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT,
        Operation.LIST_TAGS_INFO_FOR_METADATA_OBJECT
      },
      {OperationType.LIST_METADATA_OBJECTS_FOR_TAG, Operation.LIST_METADATA_OBJECTS_FOR_TAG},
      {OperationType.LIST_TAGS_INFO, Operation.LIST_TAGS_INFO},
      {OperationType.ADD_PARTITION, Operation.ADD_PARTITION},
      {OperationType.DROP_PARTITION, Operation.DROP_PARTITION},
      {OperationType.PURGE_PARTITION, Operation.PURGE_PARTITION},
      {OperationType.LOAD_PARTITION, Operation.GET_PARTITION},
      {OperationType.PARTITION_EXISTS, Operation.PARTITION_EXIST},
      {OperationType.LIST_PARTITION, Operation.LIST_PARTITION},
      {OperationType.LIST_PARTITION_NAMES, Operation.LIST_PARTITION},
      {OperationType.CREATE_FILESET, Operation.CREATE_FILESET},
      {OperationType.ALTER_FILESET, Operation.ALTER_FILESET},
      {OperationType.DROP_FILESET, Operation.DROP_FILESET},
      {OperationType.LOAD_FILESET, Operation.LOAD_FILESET},
      {OperationType.LIST_FILESET, Operation.LIST_FILESET},
      {OperationType.LIST_FILESET_FILES, Operation.LIST_FILESET_FILES},
      {OperationType.GET_FILESET_LOCATION, Operation.GET_FILE_LOCATION},
      {OperationType.CREATE_TOPIC, Operation.CREATE_TOPIC},
      {OperationType.ALTER_TOPIC, Operation.ALTER_TOPIC},
      {OperationType.DROP_TOPIC, Operation.DROP_TOPIC},
      {OperationType.LOAD_TOPIC, Operation.LOAD_TOPIC},
      {OperationType.LIST_TOPIC, Operation.LIST_TOPIC},
      {OperationType.CREATE_VIEW, Operation.CREATE_VIEW},
      {OperationType.ALTER_VIEW, Operation.ALTER_VIEW},
      {OperationType.DROP_VIEW, Operation.DROP_VIEW},
      {OperationType.LOAD_VIEW, Operation.LOAD_VIEW},
      {OperationType.VIEW_EXISTS, Operation.VIEW_EXISTS},
      {OperationType.RENAME_VIEW, Operation.RENAME_VIEW},
      {OperationType.LIST_VIEW, Operation.LIST_VIEW},
      {OperationType.REGISTER_MODEL, Operation.REGISTER_MODEL},
      {OperationType.DELETE_MODEL, Operation.DELETE_MODEL},
      {OperationType.GET_MODEL, Operation.GET_MODEL},
      {OperationType.LIST_MODEL, Operation.LIST_MODEL},
      {OperationType.ALTER_MODEL, Operation.ALTER_MODEL},
      {OperationType.LINK_MODEL_VERSION, Operation.LINK_MODEL_VERSION},
      {OperationType.DELETE_MODEL_VERSION, Operation.DELETE_MODEL_VERSION},
      {OperationType.GET_MODEL_VERSION, Operation.GET_MODEL_VERSION},
      {OperationType.GET_MODEL_VERSION_URI, Operation.GET_MODEL_VERSION_URI},
      {OperationType.LIST_MODEL_VERSIONS, Operation.LIST_MODEL_VERSIONS},
      {OperationType.LIST_MODEL_VERSION_INFOS, Operation.LIST_MODEL_VERSION_INFOS},
      {OperationType.REGISTER_AND_LINK_MODEL_VERSION, Operation.REGISTER_AND_LINK_MODEL_VERSION},
      {OperationType.ALTER_MODEL_VERSION, Operation.ALTER_MODEL_VERSION},
      {OperationType.ADD_USER, Operation.ADD_USER},
      {OperationType.REMOVE_USER, Operation.REMOVE_USER},
      {OperationType.GET_USER, Operation.GET_USER},
      {OperationType.LIST_USERS, Operation.LIST_USERS},
      {OperationType.LIST_USER_NAMES, Operation.LIST_USER_NAMES},
      {OperationType.GRANT_USER_ROLES, Operation.GRANT_USER_ROLES},
      {OperationType.REVOKE_USER_ROLES, Operation.REVOKE_USER_ROLES},
      {OperationType.ADD_GROUP, Operation.ADD_GROUP},
      {OperationType.REMOVE_GROUP, Operation.REMOVE_GROUP},
      {OperationType.GET_GROUP, Operation.GET_GROUP},
      {OperationType.LIST_GROUPS, Operation.LIST_GROUPS},
      {OperationType.LIST_GROUP_NAMES, Operation.LIST_GROUP_NAMES},
      {OperationType.GRANT_GROUP_ROLES, Operation.GRANT_GROUP_ROLES},
      {OperationType.REVOKE_GROUP_ROLES, Operation.REVOKE_GROUP_ROLES},
      {OperationType.CREATE_ROLE, Operation.CREATE_ROLE},
      {OperationType.DELETE_ROLE, Operation.DELETE_ROLE},
      {OperationType.GET_ROLE, Operation.GET_ROLE},
      {OperationType.LIST_ROLE_NAMES, Operation.LIST_ROLE_NAMES},
      {OperationType.GRANT_PRIVILEGES, Operation.GRANT_PRIVILEGES},
      {OperationType.REVOKE_PRIVILEGES, Operation.REVOKE_PRIVILEGES},
      {OperationType.OVERRIDE_PRIVILEGES, Operation.OVERRIDE_PRIVILEGES},
      {OperationType.GET_OWNER, Operation.GET_OWNER},
      {OperationType.SET_OWNER, Operation.SET_OWNER},
      {OperationType.LIST_JOB_TEMPLATES, Operation.LIST_JOB_TEMPLATES},
      {OperationType.REGISTER_JOB_TEMPLATE, Operation.REGISTER_JOB_TEMPLATE},
      {OperationType.GET_JOB_TEMPLATE, Operation.GET_JOB_TEMPLATE},
      {OperationType.ALTER_JOB_TEMPLATE, Operation.ALTER_JOB_TEMPLATE},
      {OperationType.DELETE_JOB_TEMPLATE, Operation.DELETE_JOB_TEMPLATE},
      {OperationType.LIST_JOBS, Operation.LIST_JOBS},
      {OperationType.RUN_JOB, Operation.RUN_JOB},
      {OperationType.GET_JOB, Operation.GET_JOB},
      {OperationType.CANCEL_JOB, Operation.CANCEL_JOB},
      {OperationType.LIST_STATISTICS, Operation.LIST_STATISTICS},
      {OperationType.LIST_PARTITION_STATISTICS, Operation.LIST_PARTITION_STATISTICS},
      {OperationType.DROP_STATISTICS, Operation.DROP_STATISTICS},
      {OperationType.DROP_PARTITION_STATISTICS, Operation.DROP_PARTITION_STATISTICS},
      {OperationType.UPDATE_STATISTICS, Operation.UPDATE_STATISTICS},
      {OperationType.UPDATE_PARTITION_STATISTICS, Operation.UPDATE_PARTITION_STATISTICS},
      {OperationType.CREATE_POLICY, Operation.CREATE_POLICY},
      {OperationType.GET_POLICY, Operation.GET_POLICY},
      {OperationType.ALTER_POLICY, Operation.ALTER_POLICY},
      {OperationType.DELETE_POLICY, Operation.DELETE_POLICY},
      {OperationType.ENABLE_POLICY, Operation.ENABLE_POLICY},
      {OperationType.DISABLE_POLICY, Operation.DISABLE_POLICY},
      {OperationType.LIST_POLICY, Operation.LIST_POLICY},
      {OperationType.LIST_POLICY_INFO, Operation.LIST_POLICY_INFO},
      {OperationType.LIST_METADATA_OBJECTS_FOR_POLICY, Operation.LIST_METADATA_OBJECTS_FOR_POLICY},
      {
        OperationType.LIST_POLICY_INFOS_FOR_METADATA_OBJECT,
        Operation.LIST_POLICY_INFOS_FOR_METADATA_OBJECT
      },
      {
        OperationType.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT,
        Operation.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT
      },
      {OperationType.GET_POLICY_FOR_METADATA_OBJECT, Operation.GET_POLICY_FOR_METADATA_OBJECT},
      {OperationType.UNKNOWN, Operation.UNKNOWN_OPERATION},
      {null, Operation.UNKNOWN_OPERATION}
    };

    for (Object[] testCase : testCases) {
      OperationType input = (OperationType) testCase[0];
      Operation expected = (Operation) testCase[1];
      Operation actual = CompatibilityUtils.toAuditLogOperation(input);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testAllKnownOperationTypesMapToConcreteAuditOperation() {
    EnumSet<OperationType> operationTypes = EnumSet.allOf(OperationType.class);
    operationTypes.remove(OperationType.UNKNOWN);

    for (OperationType operationType : operationTypes) {
      assertNotEquals(
          Operation.UNKNOWN_OPERATION,
          CompatibilityUtils.toAuditLogOperation(operationType),
          "OperationType should map to a concrete audit operation: " + operationType);
    }
  }

  @Test
  void testOperationStatus() {
    Assertions.assertEquals(
        Status.FAILURE, CompatibilityUtils.toAuditLogStatus(OperationStatus.FAILURE));
    Assertions.assertEquals(
        Status.SUCCESS, CompatibilityUtils.toAuditLogStatus(OperationStatus.SUCCESS));
    Assertions.assertEquals(
        Status.UNKNOWN, CompatibilityUtils.toAuditLogStatus(OperationStatus.UNPROCESSED));
    Assertions.assertEquals(
        Status.UNKNOWN, CompatibilityUtils.toAuditLogStatus(OperationStatus.UNKNOWN));
  }

  @Test
  void testOperationStatusNullHandling() {
    Assertions.assertDoesNotThrow(() -> CompatibilityUtils.toAuditLogStatus(null));
    Assertions.assertEquals(Status.UNKNOWN, CompatibilityUtils.toAuditLogStatus(null));
  }
}
