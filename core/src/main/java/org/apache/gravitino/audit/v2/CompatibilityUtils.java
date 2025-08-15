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

  private static ImmutableMap<OperationType, Operation> operationTypeMap =
      ImmutableMap.<OperationType, Operation>builder()
          // Metalake operation
          .put(OperationType.CREATE_METALAKE, Operation.CREATE_METALAKE)
          .put(OperationType.ALTER_METALAKE, Operation.ALTER_METALAKE)
          .put(OperationType.DROP_METALAKE, Operation.DROP_METALAKE)
          .put(OperationType.LOAD_METALAKE, Operation.LOAD_METALAKE)
          .put(OperationType.LIST_METALAKE, Operation.LIST_METALAKE)

          // Catalog operation
          .put(OperationType.CREATE_CATALOG, Operation.CREATE_CATALOG)
          .put(OperationType.ALTER_CATALOG, Operation.ALTER_CATALOG)
          .put(OperationType.DROP_CATALOG, Operation.DROP_CATALOG)
          .put(OperationType.LOAD_CATALOG, Operation.LOAD_CATALOG)
          .put(OperationType.LIST_CATALOG, Operation.LIST_CATALOG)

          // Schema operation
          .put(OperationType.CREATE_SCHEMA, Operation.CREATE_SCHEMA)
          .put(OperationType.ALTER_SCHEMA, Operation.ALTER_SCHEMA)
          .put(OperationType.DROP_SCHEMA, Operation.DROP_SCHEMA)
          .put(OperationType.LOAD_SCHEMA, Operation.LOAD_SCHEMA)
          .put(OperationType.LIST_SCHEMA, Operation.LIST_SCHEMA)
          .put(OperationType.SCHEMA_EXISTS, Operation.UNKNOWN_OPERATION)

          // Table operation
          .put(OperationType.CREATE_TABLE, Operation.CREATE_TABLE)
          .put(OperationType.ALTER_TABLE, Operation.ALTER_TABLE)
          .put(OperationType.DROP_TABLE, Operation.DROP_TABLE)
          .put(OperationType.PURGE_TABLE, Operation.PURGE_TABLE)
          .put(OperationType.LOAD_TABLE, Operation.LOAD_TABLE)
          .put(OperationType.TABLE_EXISTS, Operation.UNKNOWN_OPERATION)
          .put(OperationType.LIST_TABLE, Operation.LIST_TABLE)
          .put(OperationType.RENAME_TABLE, Operation.UNKNOWN_OPERATION)
          .put(OperationType.REGISTER_TABLE, Operation.UNKNOWN_OPERATION)

          // Partition operation
          .put(OperationType.ADD_PARTITION, Operation.UNKNOWN_OPERATION)
          .put(OperationType.DROP_PARTITION, Operation.UNKNOWN_OPERATION)
          .put(OperationType.PURGE_PARTITION, Operation.PURGE_PARTITION)
          .put(OperationType.LOAD_PARTITION, Operation.GET_PARTITION)
          .put(OperationType.PARTITION_EXISTS, Operation.PARTITION_EXIST)
          .put(OperationType.LIST_PARTITION, Operation.LIST_PARTITION)
          .put(OperationType.LIST_PARTITION_NAMES, Operation.LIST_PARTITION)

          // Fileset operation
          .put(OperationType.CREATE_FILESET, Operation.CREATE_FILESET)
          .put(OperationType.ALTER_FILESET, Operation.ALTER_FILESET)
          .put(OperationType.DROP_FILESET, Operation.DROP_FILESET)
          .put(OperationType.LOAD_FILESET, Operation.LOAD_FILESET)
          .put(OperationType.LIST_FILESET, Operation.LIST_FILESET)
          .put(OperationType.GET_FILESET_LOCATION, Operation.GET_FILE_LOCATION)

          // Topic operation
          .put(OperationType.CREATE_TOPIC, Operation.CREATE_TOPIC)
          .put(OperationType.ALTER_TOPIC, Operation.ALTER_TOPIC)
          .put(OperationType.DROP_TOPIC, Operation.DROP_TOPIC)
          .put(OperationType.LOAD_TOPIC, Operation.LOAD_TOPIC)
          .put(OperationType.LIST_TOPIC, Operation.LIST_TOPIC)

          // View operation
          .put(OperationType.CREATE_VIEW, Operation.UNKNOWN_OPERATION)
          .put(OperationType.ALTER_VIEW, Operation.UNKNOWN_OPERATION)
          .put(OperationType.DROP_VIEW, Operation.UNKNOWN_OPERATION)
          .put(OperationType.LOAD_VIEW, Operation.UNKNOWN_OPERATION)
          .put(OperationType.VIEW_EXISTS, Operation.UNKNOWN_OPERATION)
          .put(OperationType.RENAME_VIEW, Operation.UNKNOWN_OPERATION)
          .put(OperationType.LIST_VIEW, Operation.UNKNOWN_OPERATION)
          .build();

  static Operation toAuditLogOperation(OperationType operationType) {
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
