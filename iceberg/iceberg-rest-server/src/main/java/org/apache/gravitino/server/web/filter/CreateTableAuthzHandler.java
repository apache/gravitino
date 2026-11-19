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

import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.iceberg.rest.requests.CreateTableRequest;

/** Authorizes the destination schema and every table overlapping an explicit create location. */
public class CreateTableAuthzHandler implements AuthorizationHandler {
  private final CreateTableRequest request;

  /**
   * Creates an authorization handler for an ordinary or staged table create.
   *
   * @param request table creation request
   */
  public CreateTableAuthzHandler(CreateTableRequest request) {
    IcebergRESTUtils.validateRequest(request, "CreateTableRequest");
    this.request = request;
  }

  /** {@inheritDoc} */
  @Override
  public void process(Map<EntityType, NameIdentifier> identifiers) {
    if (!IcebergTableLocationAuthzHelper.allowed(
        identifiers, CREATE_TABLE_AUTHORIZATION_EXPRESSION, EntityType.SCHEMA)) {
      throw new ForbiddenException("Not authorized to create table in the requested schema");
    }
    if (request.location() != null) {
      IcebergTableLocationAuthzHelper.authorizeLocation(
          identifiers, request.name(), request.location());
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
    return EntityType.SCHEMA;
  }
}
