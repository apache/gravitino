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
      {OperationType.CREATE_CATALOG, Operation.CREATE_CATALOG},
      {OperationType.ALTER_CATALOG, Operation.ALTER_CATALOG},
      {OperationType.DROP_CATALOG, Operation.DROP_CATALOG},
      {OperationType.LOAD_CATALOG, Operation.LOAD_CATALOG},
      {OperationType.LIST_CATALOG, Operation.LIST_CATALOG},
      {OperationType.CREATE_SCHEMA, Operation.CREATE_SCHEMA},
      {OperationType.ALTER_SCHEMA, Operation.ALTER_SCHEMA},
      {OperationType.DROP_SCHEMA, Operation.DROP_SCHEMA},
      {OperationType.LOAD_SCHEMA, Operation.LOAD_SCHEMA},
      {OperationType.LIST_SCHEMA, Operation.LIST_SCHEMA},
      {OperationType.SCHEMA_EXISTS, Operation.UNKNOWN_OPERATION},
      {OperationType.CREATE_TABLE, Operation.CREATE_TABLE},
      {OperationType.ALTER_TABLE, Operation.ALTER_TABLE},
      {OperationType.DROP_TABLE, Operation.DROP_TABLE},
      {OperationType.PURGE_TABLE, Operation.PURGE_TABLE},
      {OperationType.LOAD_TABLE, Operation.LOAD_TABLE},
      {OperationType.TABLE_EXISTS, Operation.UNKNOWN_OPERATION},
      {OperationType.LIST_TABLE, Operation.LIST_TABLE},
      {OperationType.RENAME_TABLE, Operation.UNKNOWN_OPERATION},
      {OperationType.REGISTER_TABLE, Operation.UNKNOWN_OPERATION},
      {OperationType.ADD_PARTITION, Operation.UNKNOWN_OPERATION},
      {OperationType.DROP_PARTITION, Operation.UNKNOWN_OPERATION},
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
      {OperationType.GET_FILESET_LOCATION, Operation.GET_FILE_LOCATION},
      {OperationType.CREATE_TOPIC, Operation.CREATE_TOPIC},
      {OperationType.ALTER_TOPIC, Operation.ALTER_TOPIC},
      {OperationType.DROP_TOPIC, Operation.DROP_TOPIC},
      {OperationType.LOAD_TOPIC, Operation.LOAD_TOPIC},
      {OperationType.LIST_TOPIC, Operation.LIST_TOPIC},
      {OperationType.CREATE_VIEW, Operation.UNKNOWN_OPERATION},
      {OperationType.ALTER_VIEW, Operation.UNKNOWN_OPERATION},
      {OperationType.DROP_VIEW, Operation.UNKNOWN_OPERATION},
      {OperationType.LOAD_VIEW, Operation.UNKNOWN_OPERATION},
      {OperationType.VIEW_EXISTS, Operation.UNKNOWN_OPERATION},
      {OperationType.RENAME_VIEW, Operation.UNKNOWN_OPERATION},
      {OperationType.LIST_VIEW, Operation.UNKNOWN_OPERATION},
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
