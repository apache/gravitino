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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION;

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RegisterTableRequest;

/**
 * Handler for REGISTER_TABLE operations.
 *
 * <p>A register with {@code overwrite=true} replaces the metadata of an existing table, so it is a
 * modification of that table and not only a creation in the schema. In that mode this handler also
 * requires {@code MODIFY_TABLE} (or table ownership) on the named table, in addition to the {@code
 * CREATE_TABLE} check on the schema that the endpoint expression performs. Without this check,
 * CREATE_TABLE on a schema would allow replacing any table in it.
 *
 * <p>The mode alone decides this, without probing whether the table exists: an existence probe at
 * authorization time would race with the register that follows it. An overwrite of a missing name
 * is a plain register, which the schema-level CREATE_TABLE check still covers.
 *
 * <p>The standard expression always runs afterwards ({@link #authorizationCompleted()} returns
 * {@code false}).
 */
public class RegisterTableAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;

  /**
   * Creates a handler for a register-table request.
   *
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   */
  public RegisterTableAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    RegisterTableRequest request = extractRequest();
    if (!request.overwrite()) {
      return;
    }

    NameIdentifier schemaIdent = nameIdentifierMap.get(EntityType.SCHEMA);
    if (schemaIdent == null) {
      throw new ForbiddenException("Missing schema context for table registration authorization");
    }
    NameIdentifier tableIdent =
        NameIdentifierUtil.ofTable(
            schemaIdent.namespace().level(0),
            schemaIdent.namespace().level(1),
            schemaIdent.name(),
            request.name());

    // Evaluate on a copy so the schema-level create expression that runs next is not affected.
    Map<EntityType, NameIdentifier> tableContext = new HashMap<>(nameIdentifierMap);
    tableContext.put(EntityType.TABLE, tableIdent);
    boolean authorized =
        new AuthorizationExpressionEvaluator(MODIFY_TABLE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                tableContext,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.of(EntityType.TABLE.name()));
    if (!authorized) {
      throw new ForbiddenException(
          "User '%s' is not authorized to overwrite table '%s'",
          PrincipalUtils.getCurrentUserName(), tableIdent);
    }
  }

  @Override
  public boolean authorizationCompleted() {
    return false;
  }

  private RegisterTableRequest extractRequest() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.REGISTER_TABLE
          && args[i] instanceof RegisterTableRequest) {
        return (RegisterTableRequest) args[i];
      }
    }
    throw new ForbiddenException("RegisterTableRequest not found in method parameters");
  }
}
