/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.filter;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CREATE_TABLE_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION;

import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Authorizes staged table creation against the schema before Iceberg executes the commit.
 *
 * <p>Iceberg still validates {@code AssertTableDoesNotExist} at commit time, so this creation
 * authorization cannot replace an existing table, including one created after authorization.
 * Location is part of the new table's creation metadata; the executor's owner-property guard
 * separately sets a supplied owner to the authenticated user.
 */
public class UpdateTableAuthzHandler implements AuthorizationHandler {
  private final boolean stagedCreate;
  private final UpdateTableRequest request;

  /**
   * Creates a handler using the same staged-create predicate as the table hook dispatcher.
   *
   * @param request the table commit request
   */
  public UpdateTableAuthzHandler(UpdateTableRequest request) {
    IcebergRESTUtils.validateRequest(request, "UpdateTableRequest");
    this.request = request;
    stagedCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
  }

  /** {@inheritDoc} */
  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    String expression =
        stagedCreate
            ? CREATE_TABLE_AUTHORIZATION_EXPRESSION
            : MODIFY_TABLE_AUTHORIZATION_EXPRESSION;
    if (!IcebergTableLocationAuthzHelper.allowed(nameIdentifierMap, expression, entityType())) {
      throw new ForbiddenException(
          stagedCreate
              ? "Not authorized to create table in the requested schema"
              : "Not authorized to modify the requested table");
    }
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.SetLocation) {
        IcebergTableLocationAuthzHelper.authorizeLocation(
            nameIdentifierMap,
            nameIdentifierMap.get(EntityType.TABLE).name(),
            ((MetadataUpdate.SetLocation) update).location());
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean authorizationCompleted() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public EntityType entityType() {
    return stagedCreate ? EntityType.SCHEMA : EntityType.TABLE;
  }
}
