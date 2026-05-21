/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web.filter.authorization;

import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.requests.SchemaCreateRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Authorization executor for {@code createSchema} operations.
 *
 * <p>For nested schema names (e.g. {@code A:B:C}), injects the parent schema ({@code A:B}) into the
 * metadata context so that the standard expression can evaluate {@code CREATE_SCHEMA} against the
 * already-existing parent. {@link
 * org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer} then walks the inheritance
 * chain ({@code A:B → A → CATALOG}) automatically.
 *
 * <p>For top-level schemas no SCHEMA is injected; the expression falls back to CATALOG-level
 * checks.
 */
public class CreateSchemaAuthorizationExecutor extends CommonAuthorizerExecutor {

  /**
   * Creates an authorization executor for a {@code createSchema} request and, for nested schema
   * names, injects the parent schema into the metadata context so the standard expression can
   * evaluate {@code CREATE_SCHEMA} against the already-existing parent.
   *
   * @param parameters the parameters of the intercepted method
   * @param args the arguments passed to the intercepted method
   * @param expression the authorization expression to evaluate
   * @param metadataContext the mutable metadata context bound to the authorization expression
   * @param pathParams the path parameters of the request
   * @param entityType the optional entity type of the request
   */
  public CreateSchemaAuthorizationExecutor(
      Parameter[] parameters,
      Object[] args,
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType) {
    super(expression, metadataContext, pathParams, entityType);
    injectParentSchema(parameters, args);
  }

  private void injectParentSchema(Parameter[] parameters, Object[] args) {
    SchemaCreateRequest request = extractRequest(parameters, args);
    // Skip injection when the name is missing/blank; let SchemaCreateRequest.validate() surface the
    // intended 400 instead of throwing during authorization interception (returned as a 500).
    if (request == null || StringUtils.isBlank(request.getName())) {
      return;
    }

    NameIdentifier catalogIdent = metadataContext.get(Entity.EntityType.CATALOG);
    if (catalogIdent == null) {
      return;
    }
    String metalake = catalogIdent.namespace().level(0);
    String catalog = catalogIdent.name();

    String separator = HierarchicalSchemaUtil.schemaSeparator();
    String schemaName = request.getName();
    String[] levels = HierarchicalSchemaUtil.splitSchemaName(schemaName, separator);

    if (levels.length > 1) {
      String parentPath = String.join(separator, Arrays.copyOf(levels, levels.length - 1));
      metadataContext.put(
          Entity.EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, parentPath));
    }
  }

  private SchemaCreateRequest extractRequest(Parameter[] parameters, Object[] args) {
    for (int i = 0; i < parameters.length; i++) {
      AuthorizationRequest annotation = parameters[i].getAnnotation(AuthorizationRequest.class);
      if (annotation != null
          && annotation.type() == AuthorizationRequest.RequestType.CREATE_SCHEMA) {
        return (SchemaCreateRequest) args[i];
      }
    }
    return null;
  }
}
