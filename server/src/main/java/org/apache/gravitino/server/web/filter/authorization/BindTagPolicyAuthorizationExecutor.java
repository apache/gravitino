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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.BIND_ACCESS_POLICY_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.PolicyTagAddRequest;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyEditAuthorization;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

/** Authorizes policy association changes and records the checked policy identity. */
public class BindTagPolicyAuthorizationExecutor extends CommonAuthorizerExecutor {
  AuthorizationExpressionEvaluator accessPolicyEvaluator =
      new AuthorizationExpressionEvaluator(BIND_ACCESS_POLICY_AUTHORIZATION_EXPRESSION);
  private final Parameter[] parameters;
  private final Object[] args;
  private final boolean binding;
  boolean authorizationDisabled;

  /**
   * Creates a policy binding authorization executor.
   *
   * @param expression the governance binding expression
   * @param metadataContext metadata identifiers
   * @param pathParams request path parameters
   * @param entityType metadata type
   * @param parameters method parameters
   * @param args method arguments
   * @param binding whether the request adds an association
   */
  public BindTagPolicyAuthorizationExecutor(
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      Parameter[] parameters,
      Object[] args,
      boolean binding) {
    super(expression, metadataContext, pathParams, entityType);
    this.parameters = parameters;
    this.args = args;
    this.binding = binding;
    this.authorizationDisabled =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer()
            instanceof PassThroughAuthorizer;
  }

  /** {@inheritDoc} */
  @Override
  public boolean execute(AuthorizationRequestContext context) throws Exception {
    if (authorizationDisabled) {
      return super.execute(context);
    }
    if (binding) {
      PolicyTagAddRequest request = (PolicyTagAddRequest) extractFromParameters(parameters, args);
      try {
        (request == null ? new PolicyTagAddRequest() : request).validate();
      } catch (IllegalArgumentException e) {
        return super.execute(context);
      }
    }
    String metalake = metadataContext.get(Entity.EntityType.METALAKE).name();
    String policy = metadataContext.get(Entity.EntityType.POLICY).name();
    PolicyEntity checked;
    try {
      checked = GravitinoEnv.getInstance().policyDispatcher().getPolicy(metalake, policy);
    } catch (NoSuchPolicyException missing) {
      if (!super.execute(context)) {
        return false;
      }
      throw missing;
    }
    PolicyEditAuthorization.record(checked);
    if (binding && checked.policyType() == Policy.BuiltInType.ACCESS_CONTROL) {
      if (!super.execute(context)) {
        return false;
      }
      context.setOriginalAuthorizationExpression(BIND_ACCESS_POLICY_AUTHORIZATION_EXPRESSION);
      return accessPolicyEvaluator.evaluate(metadataContext, pathParams, context, entityType);
    }
    return super.execute(context);
  }
}
