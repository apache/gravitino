/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.lance.common.ops.LanceMetadataFilter;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.lance.namespace.errors.PermissionDeniedException;

/** Checks reads of existing metadata and filters the catalogs, schemas, and tables in listings. */
public class LanceAuthorizationMetadataFilter implements LanceMetadataFilter {

  private final String metalakeName;

  /**
   * Creates a filter bound to a metalake.
   *
   * @param metalakeName the metalake Lance REST is bound to.
   */
  public LanceAuthorizationMetadataFilter(String metalakeName) {
    this.metalakeName = metalakeName;
  }

  @Override
  public void checkReadCatalog(String catalogName) {
    checkRead(
        NameIdentifierUtil.ofCatalog(metalakeName, catalogName),
        Entity.EntityType.CATALOG,
        AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION);
  }

  @Override
  public void checkReadSchema(String catalogName, String schemaName) {
    checkRead(
        NameIdentifierUtil.ofSchema(metalakeName, catalogName, schemaName),
        Entity.EntityType.SCHEMA,
        AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION);
  }

  @Override
  public void checkReadTable(String catalogName, String schemaName, String tableName) {
    NameIdentifier identifier =
        NameIdentifierUtil.ofTable(metalakeName, catalogName, schemaName, tableName);
    try {
      // The generic lakehouse catalog manages table metadata in Gravitino's entity store. An
      // existence lookup here avoids calling the provider, whose loadTable may hydrate schemas
      // or persist dataset versions before a read permission check has succeeded.
      if (!GravitinoEnv.getInstance().entityStore().exists(identifier, Entity.EntityType.TABLE)) {
        throw new NoSuchTableException("Table %s does not exist", identifier);
      }
    } catch (IOException failure) {
      throw new RuntimeException("Failed to check table " + identifier, failure);
    }
    checkRead(
        NameIdentifierUtil.ofTable(metalakeName, catalogName, schemaName, tableName),
        Entity.EntityType.TABLE,
        AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION);
  }

  @Override
  public List<String> filterCatalogs(List<String> catalogNames) {
    return filter(
        catalogNames,
        Entity.EntityType.CATALOG,
        AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION,
        name -> NameIdentifierUtil.ofCatalog(metalakeName, name));
  }

  @Override
  public List<String> filterSchemas(String catalogName, List<String> schemaNames) {
    return filter(
        schemaNames,
        Entity.EntityType.SCHEMA,
        AuthorizationExpressionConstants.FILTER_SCHEMA_AUTHORIZATION_EXPRESSION,
        name -> NameIdentifierUtil.ofSchema(metalakeName, catalogName, name));
  }

  @Override
  public List<String> filterTables(String catalogName, String schemaName, List<String> tableNames) {
    return filter(
        tableNames,
        Entity.EntityType.TABLE,
        AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION,
        name -> NameIdentifierUtil.ofTable(metalakeName, catalogName, schemaName, name));
  }

  private void checkRead(NameIdentifier identifier, Entity.EntityType type, String expression) {
    if (!MetadataAuthzHelper.checkAccess(identifier, type, expression)) {
      throw new PermissionDeniedException(
          "Not authorized to read " + identifier, "", identifier.toString());
    }
  }

  private List<String> filter(
      List<String> names,
      Entity.EntityType entityType,
      String expression,
      Function<String, NameIdentifier> toIdentifier) {
    NameIdentifier[] identifiers = names.stream().map(toIdentifier).toArray(NameIdentifier[]::new);
    NameIdentifier[] authorized =
        MetadataAuthzHelper.filterByExpression(metalakeName, expression, entityType, identifiers);
    return Arrays.stream(authorized).map(NameIdentifier::name).collect(Collectors.toList());
  }
}
