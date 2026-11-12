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

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

/** Authorizes view loads and permits authorized probes of absent views. */
public class LoadViewAuthorizationExecutor extends CommonAuthorizerExecutor {
  private final AuthorizationExpressionEvaluator allowCheckExistenceEvaluator;

  /**
   * Creates an executor for loading views.
   *
   * @param expression the primary authorization expression
   * @param metadataContext the metadata identifiers
   * @param pathParams the request path parameters
   * @param entityType the optional entity type
   * @param allowCheckExistenceExpression the expression permitting existence probes
   */
  public LoadViewAuthorizationExecutor(
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      String allowCheckExistenceExpression) {
    super(expression, metadataContext, pathParams, entityType);
    allowCheckExistenceEvaluator =
        StringUtils.isBlank(allowCheckExistenceExpression)
            ? null
            : new AuthorizationExpressionEvaluator(allowCheckExistenceExpression);
  }

  /** {@inheritDoc} */
  @Override
  public boolean execute(AuthorizationRequestContext authorizationRequestContext) throws Exception {
    if (super.execute(authorizationRequestContext)) {
      return true;
    }

    if (allowCheckExistenceEvaluator == null
        || !allowCheckExistenceEvaluator.evaluate(
            metadataContext, pathParams, authorizationRequestContext, entityType)) {
      return false;
    }

    NameIdentifier viewIdentifier = metadataContext.get(Entity.EntityType.VIEW);
    if (viewIdentifier == null) {
      return false;
    }
    try {
      GravitinoEnv.getInstance().internalViewDispatcher().loadView(viewIdentifier);
    } catch (UnsupportedOperationException e) {
      // Preserve the primary denial when the connector cannot perform the existence probe.
    }
    return false;
  }
}
