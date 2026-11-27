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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.ACCESS_POLICY_GRANT_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

/** Authorizes creation of access-control policies. */
public class CreatePolicyAuthorizationExecutor extends CommonAuthorizerExecutor {
  private final Parameter[] parameters;
  private final Object[] args;
  AuthorizationExpressionEvaluator grantEvaluator =
      new AuthorizationExpressionEvaluator(ACCESS_POLICY_GRANT_AUTHORIZATION_EXPRESSION);

  /**
   * Creates a type-aware policy creation authorization executor.
   *
   * @param expression the ordinary creation expression
   * @param metadataContext metadata identifiers
   * @param pathParams request path parameters
   * @param entityType metadata type
   * @param parameters method parameters
   * @param args method arguments
   */
  public CreatePolicyAuthorizationExecutor(
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      Parameter[] parameters,
      Object[] args) {
    super(expression, metadataContext, pathParams, entityType);
    this.parameters = parameters;
    this.args = args;
  }

  /** {@inheritDoc} */
  @Override
  public boolean execute(AuthorizationRequestContext context) throws Exception {
    if (!super.execute(context)) {
      return false;
    }
    Object body = extractFromParameters(parameters, args);
    if (!(body instanceof PolicyCreateRequest request)
        || !Policy.BuiltInType.ACCESS_CONTROL
            .policyType()
            .equalsIgnoreCase(request.getPolicyType())) {
      return true;
    }
    context.setOriginalAuthorizationExpression(ACCESS_POLICY_GRANT_AUTHORIZATION_EXPRESSION);
    return grantEvaluator.evaluate(metadataContext, pathParams, context, entityType);
  }
}
