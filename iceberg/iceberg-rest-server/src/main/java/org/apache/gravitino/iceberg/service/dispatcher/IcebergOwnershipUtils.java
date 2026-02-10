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
package org.apache.gravitino.iceberg.service.dispatcher;

import org.apache.gravitino.Entity;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Utility class for managing ownership of Iceberg resources (schemas and tables) in Gravitino.
 * Provides centralized methods for setting ownership during resource creation operations.
 */
public class IcebergOwnershipUtils {

  /**
   * Sets the owner of a schema (namespace) to the specified user using the provided owner
   * dispatcher.
   *
   * @param metalake the metalake name
   * @param catalogName the catalog name
   * @param namespace the Iceberg namespace
   * @param user the user to set as owner
   * @param ownerDispatcher the owner dispatcher to use
   */
  public static void setSchemaOwner(
      String metalake,
      String catalogName,
      Namespace namespace,
      String user,
      OwnerDispatcher ownerDispatcher) {
    if (ownerDispatcher != null) {
      ownerDispatcher.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              IcebergIdentifierUtils.toGravitinoSchemaIdentifier(metalake, catalogName, namespace),
              Entity.EntityType.SCHEMA),
          user,
          Owner.Type.USER);
    }
  }

  /**
   * Sets the owner of a table to the specified user using the provided owner dispatcher.
   *
   * @param metalake the metalake name
   * @param catalogName the catalog name
   * @param namespace the Iceberg namespace containing the table
   * @param tableName the table name
   * @param user the user to set as owner
   * @param ownerDispatcher the owner dispatcher to use
   */
  public static void setTableOwner(
      String metalake,
      String catalogName,
      Namespace namespace,
      String tableName,
      String user,
      OwnerDispatcher ownerDispatcher) {
    if (ownerDispatcher != null) {
      ownerDispatcher.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              IcebergIdentifierUtils.toGravitinoTableIdentifier(
                  metalake, catalogName, TableIdentifier.of(namespace, tableName)),
              Entity.EntityType.TABLE),
          user,
          Owner.Type.USER);
    }
  }
}
