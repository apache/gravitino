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

import static org.apache.gravitino.server.web.filter.ParameterUtil.buildNameIdentifierForBatchAuthorization;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import com.google.common.base.Preconditions;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.PoliciesAssociateRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

/**
 * Metadata object authorization for {@link
 * org.apache.gravitino.server.web.rest.MetadataObjectPolicyOperations#associatePoliciesForObject(String,
 * String, String, PoliciesAssociateRequest)}
 */
public class AssociatePolicyAuthorizationExecutor extends CommonAuthorizerExecutor
    implements AuthorizationExecutor {

  private final Parameter[] parameters;
  private final Object[] args;

  public AssociatePolicyAuthorizationExecutor(
      String expression,
      Parameter[] parameters,
      Object[] args,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      AuthorizationExpressionEvaluator authorizationExpressionEvaluator,
      Map<String, Object> pathParams,
      String entityType) {
    super(expression, metadataContext, authorizationExpressionEvaluator, pathParams, entityType);
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public boolean execute() throws Exception {
    Object request = extractFromParameters(parameters, args);
    if (request == null) {
      return false;
    }

    AuthorizationRequestContext context = new AuthorizationRequestContext();
    context.setOriginalAuthorizationExpression(expression);
    Entity.EntityType targetType =
        Entity.EntityType.POLICY; // policies are the only supported batch target here
    Preconditions.checkArgument(
        request instanceof PoliciesAssociateRequest,
        "Only policy can use AssociatePolicyAuthorizationExecutor, please contact the administrator.");
    PoliciesAssociateRequest policiesAssociateRequest = (PoliciesAssociateRequest) request;
    policiesAssociateRequest.validate();
    // Authorize both 'policiesToAdd' and 'policiesToRemove' fields.
    return authorizePolicy(policiesAssociateRequest.getPoliciesToAdd(), context, targetType)
        && authorizePolicy(policiesAssociateRequest.getPoliciesToRemove(), context, targetType);
  }

  /**
   * Performs batch authorization for a given field (e.g., "policiesToAdd" or "policiesToRemove")
   * containing an array of tag names.
   *
   * @param policies policies
   * @param context The shared authorization request context.
   * @param targetType The entity type being authorized (expected to be TAG).
   * @return {@code true} if all policies in the field pass authorization; {@code false} otherwise.
   */
  private boolean authorizePolicy(
      String[] policies, AuthorizationRequestContext context, Entity.EntityType targetType) {

    // Treat null or empty arrays as no-op (implicitly authorized)
    if (policies == null) {
      return true;
    }

    for (String policy : policies) {
      // Use a fresh context copy for each tag to avoid cross-contamination
      Map<Entity.EntityType, NameIdentifier> currentContext = new HashMap<>(this.metadataContext);
      buildNameIdentifierForBatchAuthorization(currentContext, policy, targetType);

      boolean authorized =
          authorizationExpressionEvaluator.evaluate(
              currentContext, pathParams, context, Optional.ofNullable(entityType));

      if (!authorized) {
        return false; // Fail fast on first unauthorized tag
      }
    }
    return true;
  }
}
