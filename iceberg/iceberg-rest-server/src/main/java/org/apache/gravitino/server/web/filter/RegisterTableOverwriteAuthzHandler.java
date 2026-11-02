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
 * Authorizes Iceberg REST {@code registerTable} when {@code overwrite=true}.
 *
 * <p>Overwrite replaces the metadata pointer of an existing table name, so it is authorized as a
 * table modification ({@code TABLE::OWNER} or {@code MODIFY_TABLE}) rather than as schema-level
 * {@code CREATE_TABLE}. The overwrite flag alone decides this, without probing whether the table
 * exists: an existence check at authorization time would race with the register that follows, and
 * the required privilege would then depend on that race. The same approach is used for Lance create
 * overwrite.
 *
 * <p>When {@code overwrite} is false, this handler does nothing and the method annotation's create
 * expression still applies.
 */
@SuppressWarnings("FormatStringAnnotation")
public class RegisterTableOverwriteAuthzHandler implements AuthorizationHandler {

  private final Parameter[] parameters;
  private final Object[] args;
  private boolean overwrite;

  /**
   * Creates a handler for the register-table request arguments.
   *
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   */
  public RegisterTableOverwriteAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    RegisterTableRequest request = registerTableRequest();
    overwrite = request.overwrite();
    if (!overwrite) {
      return;
    }

    NameIdentifier metalakeIdent = nameIdentifierMap.get(EntityType.METALAKE);
    NameIdentifier catalogIdent = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schemaIdent = nameIdentifierMap.get(EntityType.SCHEMA);
    if (metalakeIdent == null || catalogIdent == null || schemaIdent == null) {
      throw new ForbiddenException(
          "Missing metalake, catalog, or schema context for register-table overwrite authorization");
    }

    String metalakeName = metalakeIdent.name();
    String catalog = catalogIdent.name();
    String schema = schemaIdent.name();
    String tableName = request.name();
    nameIdentifierMap.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalakeName, catalog, schema, tableName));

    boolean authorized =
        new AuthorizationExpressionEvaluator(MODIFY_TABLE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                nameIdentifierMap,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.of(EntityType.TABLE.name()));
    if (!authorized) {
      throw new ForbiddenException(
          "User '%s' is not authorized to overwrite table '%s'",
          PrincipalUtils.getCurrentUserName(), nameIdentifierMap.get(EntityType.TABLE));
    }
  }

  @Override
  public boolean authorizationCompleted() {
    // Overwrite is fully authorized here, so the create expression on the method must not run.
    return overwrite;
  }

  private RegisterTableRequest registerTableRequest() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.REGISTER_TABLE) {
        return (RegisterTableRequest) args[i];
      }
    }
    throw new ForbiddenException("RegisterTableRequest not found in parameters");
  }
}
