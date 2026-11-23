/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
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
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.AuthorizationRequestScope;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

/** Authorizes credential retrieval using metadata access and static-provider privileges. */
public class CredentialAuthorizationExecutor extends CommonAuthorizerExecutor {
  private final String staticCredentialExpression;
  @Nullable private final AuthorizationExpressionEvaluator staticCredentialEvaluator;

  /**
   * Creates an executor using the expressions declared on the credential operation.
   *
   * @param expression the metadata access expression
   * @param metadataContext the metadata identifiers
   * @param pathParams the request path parameters
   * @param entityType the requested metadata object type
   * @param secondaryExpression the privileges required for static catalog credentials
   * @param secondaryExpressionCondition when the static credential expression applies
   */
  public CredentialAuthorizationExecutor(
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      String secondaryExpression,
      ExpressionCondition secondaryExpressionCondition) {
    super(expression, metadataContext, pathParams, entityType);
    staticCredentialExpression = secondaryExpression;
    staticCredentialEvaluator =
        secondaryExpressionCondition == ExpressionCondition.CATALOG_CREDENTIALS
            ? new AuthorizationExpressionEvaluator(secondaryExpression)
            : null;
  }

  @Override
  public boolean execute(AuthorizationRequestContext context) throws Exception {
    boolean authorized = super.execute(context);
    if (!authorized || entityType.filter(Entity.EntityType.CATALOG.name()::equals).isEmpty()) {
      return authorized;
    }
    if (staticCredentialEvaluator != null
        && staticCredentialEvaluator.evaluate(metadataContext, pathParams, context, entityType)) {
      return true;
    }
    NameIdentifier catalog = metadataContext.get(Entity.EntityType.CATALOG);
    context.setOriginalAuthorizationExpression(staticCredentialExpression);
    AuthorizationRequestScope.requireDynamicCredentials(catalog);
    return GravitinoEnv.getInstance()
        .catalogManager()
        .doWithCatalog(
            catalog,
            baseCatalog ->
                !baseCatalog.catalogCredentialManager().hasOnlyStaticCredentialProviders());
  }
}
