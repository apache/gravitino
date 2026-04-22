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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;

/**
 * Handler for CREATE_NAMESPACE operations that require parent-scope authorization. For nested
 * namespace creation (e.g., {@code A:B:C}), authorization is checked against the parent schema
 * ({@code A:B}) rather than the namespace being created. Privilege inheritance in {@link
 * org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer} then automatically
 * propagates the check up the ancestor chain ({@code A:B} → {@code A} → CATALOG → METALAKE).
 *
 * <p>For top-level namespace creation (e.g., {@code A}), the standard CATALOG-level
 * {@code CREATE_SCHEMA} check applies.
 */
@SuppressWarnings("FormatStringAnnotation")
public class CreateNamespaceAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;

  public CreateNamespaceAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    Namespace namespace = extractNamespace();
    NameIdentifier catalogId = nameIdentifierMap.get(EntityType.CATALOG);
    if (catalogId == null) {
      throw new ForbiddenException("Missing catalog context for namespace creation authorization");
    }
    String metalake = catalogId.namespace().level(0);
    String catalog = catalogId.name();
    String separator = HierarchicalSchemaUtil.namespaceSeparator();
    String fullPath = String.join(separator, namespace.levels());

    // For nested namespaces (e.g., "A:B:C"), check CREATE_SCHEMA on the parent ("A:B").
    // JcasbinAuthorizer inheritance then walks A:B → A → CATALOG automatically.
    if (HierarchicalSchemaUtil.isNested(fullPath, separator)) {
      String parentPath = fullPath.substring(0, fullPath.lastIndexOf(separator));
      nameIdentifierMap.put(
          EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, parentPath));
    }

    performAuthorization(nameIdentifierMap);
  }

  @Override
  public boolean authorizationCompleted() {
    return true;
  }

  private void performAuthorization(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    boolean authorized =
        new AuthorizationExpressionEvaluator(
                AuthorizationExpressionConstants.CREATE_NAMESPACE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                nameIdentifierMap,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.empty());
    if (!authorized) {
      throw new ForbiddenException(
          "User '%s' is not authorized to create namespace", PrincipalUtils.getCurrentUserName());
    }
  }

  private Namespace extractNamespace() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.CREATE_NAMESPACE) {
        return ((CreateNamespaceRequest) args[i]).namespace();
      }
    }
    throw new ForbiddenException("CreateNamespaceRequest not found in parameters");
  }
}
