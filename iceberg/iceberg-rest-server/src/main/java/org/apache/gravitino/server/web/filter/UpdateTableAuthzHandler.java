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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/** Authorizes staged table creation against the schema before Iceberg executes the commit. */
public class UpdateTableAuthzHandler implements AuthorizationHandler {
  private final boolean stagedCreate;

  /**
   * Creates a handler using the same staged-create predicate as the table hook dispatcher.
   *
   * @param request the table commit request
   */
  public UpdateTableAuthzHandler(UpdateTableRequest request) {
    IcebergRESTUtils.validateRequest(request, "UpdateTableRequest");
    stagedCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    if (stagedCreate
        && !new AuthorizationExpressionEvaluator(CREATE_TABLE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                nameIdentifierMap,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.of(EntityType.SCHEMA.name()))) {
      throw new ForbiddenException(
          "Not authorized to create table in schema '%s'",
          nameIdentifierMap.get(EntityType.SCHEMA));
    }
  }

  @Override
  public boolean authorizationCompleted() {
    // Ordinary commits continue through the endpoint's existing MODIFY_TABLE expression.
    return stagedCreate;
  }

  @Override
  public EntityType entityType() {
    return stagedCreate ? EntityType.SCHEMA : EntityType.TABLE;
  }
}
