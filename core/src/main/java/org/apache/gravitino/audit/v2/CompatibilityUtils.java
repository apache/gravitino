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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.audit.AuditLog.Operation;
import org.apache.gravitino.audit.AuditLog.Status;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;

public class CompatibilityUtils {

  private static Map<OperationType, Operation> operationTypeMap = new HashMap<>();

  static {
    initOperationTypeMap();
  }

  static void initOperationTypeMap() {
    // Metalake operation
    operationTypeMap.put(OperationType.CREATE_METALAKE, Operation.CREATE_METALAKE);
    operationTypeMap.put(OperationType.ALTER_METALAKE, Operation.ALTER_METALAKE);
    operationTypeMap.put(OperationType.DROP_METALAKE, Operation.DROP_METALAKE);
    operationTypeMap.put(OperationType.LOAD_METALAKE, Operation.LOAD_METALAKE);
    operationTypeMap.put(OperationType.LIST_METALAKE, Operation.LIST_METALAKE);

    // Catalog operation
    operationTypeMap.put(OperationType.CREATE_CATALOG, Operation.CREATE_CATALOG);
    operationTypeMap.put(OperationType.ALTER_CATALOG, Operation.ALTER_CATALOG);
    operationTypeMap.put(OperationType.DROP_CATALOG, Operation.DROP_CATALOG);
    operationTypeMap.put(OperationType.LOAD_CATALOG, Operation.LOAD_CATALOG);
    operationTypeMap.put(OperationType.LIST_CATALOG, Operation.LIST_CATALOG);

    // Schema operation
    operationTypeMap.put(OperationType.CREATE_SCHEMA, Operation.CREATE_SCHEMA);
    operationTypeMap.put(OperationType.ALTER_SCHEMA, Operation.ALTER_SCHEMA);
    operationTypeMap.put(OperationType.DROP_SCHEMA, Operation.DROP_SCHEMA);
    operationTypeMap.put(OperationType.LOAD_SCHEMA, Operation.LOAD_SCHEMA);
    operationTypeMap.put(OperationType.LIST_SCHEMA, Operation.LIST_SCHEMA);

    // Table operation
    operationTypeMap.put(OperationType.CREATE_TABLE, Operation.CREATE_TABLE);
    operationTypeMap.put(OperationType.ALTER_TABLE, Operation.ALTER_TABLE);
    operationTypeMap.put(OperationType.DROP_TABLE, Operation.DROP_TABLE);
    operationTypeMap.put(OperationType.PURGE_TABLE, Operation.PURGE_TABLE);
    operationTypeMap.put(OperationType.LOAD_TABLE, Operation.LOAD_TABLE);
    operationTypeMap.put(OperationType.TABLE_EXISTS, Operation.UNKNOWN_OPERATION);
    operationTypeMap.put(OperationType.LIST_TABLE, Operation.LIST_TABLE);

    // Partition operation
    operationTypeMap.put(OperationType.ADD_PARTITION, Operation.UNKNOWN_OPERATION);
    operationTypeMap.put(OperationType.DROP_PARTITION, Operation.UNKNOWN_OPERATION);
    operationTypeMap.put(OperationType.PURGE_PARTITION, Operation.PURGE_PARTITION);
    operationTypeMap.put(OperationType.LOAD_PARTITION, Operation.GET_PARTITION);
    operationTypeMap.put(OperationType.PARTITION_EXISTS, Operation.PARTITION_EXIST);
    operationTypeMap.put(OperationType.LIST_PARTITION, Operation.LIST_PARTITION);
    operationTypeMap.put(OperationType.LIST_PARTITION_NAMES, Operation.LIST_PARTITION);

    // Fileset operation
    operationTypeMap.put(OperationType.CREATE_FILESET, Operation.CREATE_FILESET);
    operationTypeMap.put(OperationType.ALTER_FILESET, Operation.ALTER_FILESET);
    operationTypeMap.put(OperationType.DROP_FILESET, Operation.DROP_FILESET);
    operationTypeMap.put(OperationType.LOAD_FILESET, Operation.LOAD_FILESET);
    operationTypeMap.put(OperationType.LIST_FILESET, Operation.LIST_FILESET);
    operationTypeMap.put(OperationType.GET_FILESET_LOCATION, Operation.GET_FILE_LOCATION);

    // Topic operation
    operationTypeMap.put(OperationType.CREATE_TOPIC, Operation.CREATE_TOPIC);
    operationTypeMap.put(OperationType.ALTER_TOPIC, Operation.ALTER_TOPIC);
    operationTypeMap.put(OperationType.DROP_TOPIC, Operation.DROP_TOPIC);
    operationTypeMap.put(OperationType.LOAD_TOPIC, Operation.LOAD_TOPIC);
    operationTypeMap.put(OperationType.LIST_TOPIC, Operation.LIST_TOPIC);
  }

  static Operation toAuditLogOperation(OperationType operationType) {
    if (operationTypeMap.containsKey(operationType)) {
      return operationTypeMap.get(operationType);
    }

    return Operation.UNKNOWN_OPERATION;
  }

  static Status toAuditLogStatus(OperationStatus operationStatus) {
    if (operationStatus.equals(OperationStatus.SUCCESS)) {
      return Status.SUCCESS;
    } else if (operationStatus.equals(OperationStatus.FAILURE)) {
      return Status.FAILURE;
    } else {
      return Status.UNKNOWN;
    }
  }
}
