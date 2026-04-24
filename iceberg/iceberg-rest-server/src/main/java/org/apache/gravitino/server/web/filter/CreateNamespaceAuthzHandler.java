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
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;

/**
 * Handler for CREATE_NAMESPACE operations. For nested namespace creation (e.g. {@code A:B:C}),
 * injects the parent schema ({@code A:B}) into the authorization context so that the standard
 * expression can check {@code CREATE_SCHEMA} against the already-existing parent.
 *
 * <p>For top-level namespaces no SCHEMA is injected and the standard expression falls back to
 * CATALOG-level checks. Authorization is completed by the standard expression evaluator ({@link
 * #authorizationCompleted()} returns {@code false}).
 */
public class CreateNamespaceAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;

  public CreateNamespaceAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    CreateNamespaceRequest request = extractRequest();

    NameIdentifier catalogIdent = nameIdentifierMap.get(EntityType.CATALOG);
    if (catalogIdent == null) {
      throw new ForbiddenException("Missing catalog context for namespace creation authorization");
    }
    String metalake = catalogIdent.namespace().level(0);
    String catalog = catalogIdent.name();

    Namespace namespace = request.namespace();
    int levels = namespace.length();

    String separator = HierarchicalSchemaUtil.namespaceSeparator();

    // For nested namespaces inject the parent schema so the expression evaluates against it.
    // JcasbinAuthorizer inheritance then walks A:B → A → CATALOG automatically.
    // For top-level namespaces no SCHEMA is injected; the expression uses CATALOG-level checks.
    if (levels > 1) {
      String parentPath = String.join(separator, Arrays.copyOf(namespace.levels(), levels - 1));
      nameIdentifierMap.put(
          EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, parentPath));
    }
  }

  @Override
  public boolean authorizationCompleted() {
    return false;
  }

  private CreateNamespaceRequest extractRequest() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.CREATE_NAMESPACE) {
        return (CreateNamespaceRequest) args[i];
      }
    }
    throw new ForbiddenException("CreateNamespaceRequest not found in method parameters");
  }
}
