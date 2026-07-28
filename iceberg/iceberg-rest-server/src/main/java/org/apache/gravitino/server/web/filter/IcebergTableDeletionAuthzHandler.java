/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.Map;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTUtil;

/** Authorizes a table operation that may address a retained table row. */
public class IcebergTableDeletionAuthzHandler implements AuthorizationHandler {

  private static final String REQUIRED_PARENT_USE = "ANY_USE_CATALOG && ANY_USE_SCHEMA";
  private static final String AUTHORIZED_DELETION_ATTRIBUTE =
      IcebergTableDeletionAuthzHandler.class.getName() + ".authorizedDeletion";

  private final AuthorizationExpression authorizationExpression;
  private final Parameter[] parameters;
  private final Object[] args;
  private boolean authorizationCompleted;

  /**
   * Creates an authorization handler for a deletion-aware table operation.
   *
   * @param authorizationExpression authorization declared by the REST operation
   * @param parameters REST method parameters
   * @param args REST method arguments
   */
  public IcebergTableDeletionAuthzHandler(
      AuthorizationExpression authorizationExpression, Parameter[] parameters, Object[] args) {
    this.authorizationExpression = authorizationExpression;
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    String tableName = extractTableName();
    NameIdentifier catalog = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schema = nameIdentifierMap.get(EntityType.SCHEMA);
    if (tableName == null || catalog == null || schema == null) {
      throw new NoSuchTableException("Table does not exist");
    }

    NameIdentifier table =
        NameIdentifierUtil.ofTable(
            catalog.namespace().level(0), catalog.name(), schema.name(), tableName);
    nameIdentifierMap.put(EntityType.TABLE, table);

    TablePO retained = findRetainedTable(schema, tableName);
    if (retained == null) {
      // Let the interceptor evaluate the ordinary live-table expression.
      return;
    }

    authorizationCompleted = true;
    String expression =
        authorizationExpression == null || authorizationExpression.expression().isBlank()
            ? AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION
            : authorizationExpression.expression();
    if (!canManageRetained(table, retained.getTableId(), expression)) {
      // A retained name is not disclosed to callers who cannot manage that exact table identity.
      throw new NoSuchTableException("Table does not exist: %s", tableName);
    }
    HttpServletRequest request = extractHttpRequest();
    if (request == null) {
      throw new IllegalStateException(
          "Deletion-aware Iceberg operations must expose their HTTP request to authorization");
    }
    request.setAttribute(
        AUTHORIZED_DELETION_ATTRIBUTE,
        new AuthorizedDeletionTarget(retained.getTableId(), retained.getDeletionId()));
  }

  @Override
  public boolean authorizationCompleted() {
    return authorizationCompleted;
  }

  /**
   * Checks whether the current principal may see one retained table in deletion-list output.
   *
   * @param identifier original table identifier
   * @param retainedTableId immutable retained table ID
   * @return whether the retained table is visible to the current principal
   */
  public static boolean canListRetained(NameIdentifier identifier, long retainedTableId) {
    return canManageRetained(
        identifier,
        retainedTableId,
        AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION);
  }

  /**
   * Returns the exact retained deletion generation authorized for this request.
   *
   * @param request current HTTP request
   * @return authorized retained target, or {@code null} when authorization observed a live name
   */
  @Nullable
  public static AuthorizedDeletionTarget authorizedDeletion(HttpServletRequest request) {
    Object value = request.getAttribute(AUTHORIZED_DELETION_ATTRIBUTE);
    return value instanceof AuthorizedDeletionTarget ? (AuthorizedDeletionTarget) value : null;
  }

  /** Exact retained identity authorized for one request. */
  public static final class AuthorizedDeletionTarget {
    private final long tableId;
    private final String deletionId;

    /**
     * Creates one exact retained target.
     *
     * @param tableId immutable source table ID
     * @param deletionId exact deletion generation ID
     */
    public AuthorizedDeletionTarget(long tableId, String deletionId) {
      this.tableId = tableId;
      this.deletionId = deletionId;
    }

    /**
     * Returns the immutable source table ID used for authorization.
     *
     * @return immutable source table ID
     */
    public long tableId() {
      return tableId;
    }

    /**
     * Returns the exact deletion generation ID.
     *
     * @return deletion generation ID
     */
    public String deletionId() {
      return deletionId;
    }
  }

  private static boolean canManageRetained(
      NameIdentifier identifier, long retainedTableId, String expression) {
    IcebergRESTServerContext context = IcebergRESTServerContext.getInstance();
    if (!context.isAuthorizationEnabled()) {
      return true;
    }
    if (MetadataAuthzHelper.checkAccess(identifier, EntityType.TABLE, expression)) {
      return true;
    }
    return MetadataAuthzHelper.checkAccess(identifier, EntityType.TABLE, REQUIRED_PARENT_USE)
        && TableDeletionService.getInstance()
            .isRetainedOwner(retainedTableId, PrincipalUtils.getCurrentPrincipal());
  }

  @Nullable
  private static TablePO findRetainedTable(NameIdentifier schema, String tableName) {
    if (!IcebergRESTServerContext.getInstance().isAuxMode()) {
      return null;
    }
    try {
      long schemaId = EntityIdService.getEntityId(schema, Entity.EntityType.SCHEMA);
      return TableDeletionService.getInstance().getRetainedTable(schemaId, tableName);
    } catch (NoSuchEntityException e) {
      return null;
    }
  }

  @Nullable
  private String extractTableName() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null && metadata.type() == RequestType.MANAGE_TABLE_DELETION) {
        return RESTUtil.decodeString(String.valueOf(args[i]));
      }
    }
    return null;
  }

  @Nullable
  private HttpServletRequest extractHttpRequest() {
    for (int i = 0; i < parameters.length; i++) {
      if (HttpServletRequest.class.isAssignableFrom(parameters[i].getType())) {
        return (HttpServletRequest) args[i];
      }
    }
    return null;
  }
}
