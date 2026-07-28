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
package org.apache.gravitino.iceberg.service.deletion;

import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.utils.PrincipalUtils;

/** Authorization bridge for operations on a retained table deletion generation. */
public final class IcebergDeletionAuthorization {

  private static final String REQUIRED_PARENT_USE = "ANY_USE_CATALOG && ANY_USE_SCHEMA";

  private IcebergDeletionAuthorization() {}

  /**
   * Checks current {@code DROP_TABLE} semantics against a live or retained table generation.
   *
   * <p>The retained-owner fallback is fenced by the immutable table ID and deletion ID, and still
   * requires current parent-use privileges.
   *
   * @param identifier full Gravitino table identifier
   * @param deletion exact retained root and action, or {@code null} for a live or missing table
   * @return whether the current principal may manage the deletion
   */
  public static boolean canDrop(
      NameIdentifier identifier, @Nullable IcebergRetainedTableDeletion deletion) {
    IcebergRESTServerContext context = IcebergRESTServerContext.getInstance();
    String catalogName = identifier.namespace().level(1);
    if (!context.isAuthorizationEnabled() || context.shouldSkipAuthorization(catalogName)) {
      return true;
    }
    if (MetadataAuthzHelper.checkAccess(
        identifier,
        Entity.EntityType.TABLE,
        AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION)) {
      return true;
    }
    return deletion != null
        && MetadataAuthzHelper.checkAccess(identifier, Entity.EntityType.TABLE, REQUIRED_PARENT_USE)
        && TableDeletionService.getInstance()
            .isRetainedOwner(
                deletion.getTable().getTableId(), PrincipalUtils.getCurrentPrincipal());
  }
}
