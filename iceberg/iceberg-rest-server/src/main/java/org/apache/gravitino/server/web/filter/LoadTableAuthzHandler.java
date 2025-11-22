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

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Handler for LOAD_TABLE operations. Validates that the requested table is not a metadata table
 * (e.g., table$snapshots) and extracts the table identifier for authorization checks.
 */
public class LoadTableAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;

  public LoadTableAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    // Find the table name and namespace from parameters
    String tableName = null;
    Namespace namespace = null;

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];

      IcebergAuthorizationMetadata icebergMetadata =
          parameter.getAnnotation(IcebergAuthorizationMetadata.class);
      if (icebergMetadata != null && icebergMetadata.type() == RequestType.LOAD_TABLE) {
        tableName = String.valueOf(args[i]);
      }

      AuthorizationMetadata authMetadata = parameter.getAnnotation(AuthorizationMetadata.class);
      if (authMetadata != null && authMetadata.type() == EntityType.SCHEMA) {
        // Decode the raw Iceberg namespace parameter
        namespace = RESTUtil.decodeNamespace(String.valueOf(args[i]));
      }
    }

    if (tableName == null || namespace == null) {
      throw new NoSuchTableException("Table not found - missing table name or namespace");
    }

    // Validate that this is not a metadata table access
    if (isMetadataTable(tableName, namespace)) {
      throw new NoSuchTableException("Table %s not found", tableName);
    }

    NameIdentifier catalogId = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schemaId = nameIdentifierMap.get(EntityType.SCHEMA);

    if (catalogId == null || schemaId == null) {
      throw new NoSuchTableException("Missing catalog or schema context for table authorization");
    }

    String metalakeName = catalogId.namespace().level(0);
    String catalog = catalogId.name();
    String schema = schemaId.name();

    // Add table identifier to the map for authorization expression evaluation
    nameIdentifierMap.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalakeName, catalog, schema, tableName));
  }

  @Override
  public boolean authorizationCompleted() {
    // This handler only enriches identifiers, doesn't perform full authorization
    return false;
  }

  /**
   * Check if the table is a metadata table. Metadata tables have special names like
   * `table$snapshots`, `table$files`, etc., and are accessed with longer namespace paths
   * (catalog.db.table instead of catalog.db).
   *
   * @param tableName The table name to check
   * @param namespace The Iceberg namespace of the table
   * @return true if this is a metadata table access, false otherwise
   */
  private boolean isMetadataTable(String tableName, Namespace namespace) {
    MetadataTableType metadataTableType = MetadataTableType.from(tableName);
    if (metadataTableType == null) {
      return false;
    }

    // Metadata tables have namespace length > 1 (e.g., catalog.db.table has 3 levels)
    // Regular tables have namespace length = 1 (e.g., catalog.db has 2 levels, but we get "db")
    if (namespace.levels().length > 1) {
      return true;
    }
    return false;
  }
}
