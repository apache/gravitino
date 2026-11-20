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
package org.apache.gravitino.server.web.filter.authorization;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.BIND_ACCESS_POLICY_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.PolicySetRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdatesRequest;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.AccessControlContent;
import org.apache.gravitino.policy.HiddenPolicyTag;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyEditAuthorization;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Authorizes changes that widen or enable bound access policies. */
public class AlterPolicyAuthorizationExecutor extends CommonAuthorizerExecutor {
  private final Parameter[] parameters;
  private final Object[] args;
  AuthorizationExpressionEvaluator accessPolicyEvaluator =
      new AuthorizationExpressionEvaluator(BIND_ACCESS_POLICY_AUTHORIZATION_EXPRESSION);
  boolean authorizationDisabled;

  /**
   * Creates an authorization executor for policy edits.
   *
   * @param expression the policy edit expression
   * @param metadataContext metadata identifiers
   * @param pathParams request path parameters
   * @param entityType metadata type
   * @param parameters method parameters
   * @param args method arguments
   */
  public AlterPolicyAuthorizationExecutor(
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      Parameter[] parameters,
      Object[] args) {
    super(expression, metadataContext, pathParams, entityType);
    this.parameters = parameters;
    this.args = args;
    this.authorizationDisabled =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer()
            instanceof PassThroughAuthorizer;
  }

  /** {@inheritDoc} */
  @Override
  public boolean execute(AuthorizationRequestContext context) throws Exception {
    if (!super.execute(context)) {
      return false;
    }
    if (authorizationDisabled) {
      return true;
    }

    Object body = extractFromParameters(parameters, args);
    String metalake = metadataContext.get(Entity.EntityType.METALAKE).name();
    String policyName = metadataContext.get(Entity.EntityType.POLICY).name();
    if (body instanceof PolicySetRequest request) {
      return !request.isEnable() || authorizeEnable(metalake, policyName, context);
    }
    if (!(body instanceof PolicyUpdatesRequest request)) {
      return true;
    }
    try {
      request.validate();
      for (PolicyUpdateRequest update : request.getUpdates()) {
        if (update instanceof PolicyUpdateRequest.RenamePolicyRequest) {
          NameIdentifierUtil.checkMetadataObjectName(
              NameIdentifierUtil.ofPolicy(
                  metalake, ((PolicyUpdateRequest.RenamePolicyRequest) update).getNewName()),
              Entity.EntityType.POLICY);
        }
      }
    } catch (IllegalArgumentException e) {
      return true;
    }

    Optional<PolicyChange.UpdateContent> finalUpdate =
        request.getUpdates().stream()
            .filter(update -> update instanceof PolicyUpdateRequest.UpdatePolicyContentRequest)
            .map(update -> (PolicyChange.UpdateContent) update.policyChange())
            .reduce((first, last) -> last);
    if (finalUpdate.isEmpty()) {
      return true;
    }

    PolicyEntity policy =
        GravitinoEnv.getInstance().policyDispatcher().getPolicy(metalake, policyName);
    PolicyEditAuthorization.record(policy);
    if (policy.policyType() != Policy.BuiltInType.ACCESS_CONTROL) {
      return true;
    }
    // Every update must match the immutable policy type, including updates overwritten later.
    for (PolicyUpdateRequest update : request.getUpdates()) {
      if (update instanceof PolicyUpdateRequest.UpdatePolicyContentRequest) {
        PolicyChange.UpdateContent change = (PolicyChange.UpdateContent) update.policyChange();
        if (Policy.BuiltInType.fromPolicyType(change.getPolicyType()) != policy.policyType()
            || !policy
                .content()
                .supportedObjectTypes()
                .equals(change.getContent().supportedObjectTypes())) {
          return true;
        }
      }
    }
    if (!(finalUpdate.get().getContent() instanceof AccessControlContent)) {
      return true;
    }
    AccessControlContent oldContent = (AccessControlContent) policy.content();
    AccessControlContent newContent = (AccessControlContent) finalUpdate.get().getContent();
    return !isWidening(oldContent, newContent)
        || authorizeBoundAccessChange(metalake, policyName, context);
  }

  private boolean authorizeEnable(
      String metalake, String policyName, AuthorizationRequestContext context) throws Exception {
    PolicyEntity policy =
        GravitinoEnv.getInstance().policyDispatcher().getPolicy(metalake, policyName);
    PolicyEditAuthorization.record(policy);
    if (policy.policyType() != Policy.BuiltInType.ACCESS_CONTROL) {
      return true;
    }
    if (policy.enabled()) {
      PolicyEditAuthorization.requireEnabled();
      return true;
    }
    return authorizeBoundAccessChange(metalake, policyName, context);
  }

  private boolean authorizeBoundAccessChange(
      String metalake, String policyName, AuthorizationRequestContext context) throws Exception {
    if (!isBound(metalake, policyName)) {
      PolicyEditAuthorization.requireUnbound();
      return true;
    }
    context.setOriginalAuthorizationExpression(BIND_ACCESS_POLICY_AUTHORIZATION_EXPRESSION);
    return accessPolicyEvaluator.evaluate(metadataContext, pathParams, context, entityType);
  }

  static boolean isWidening(AccessControlContent oldContent, AccessControlContent newContent) {
    return !oldContent.privileges().containsAll(newContent.privileges())
        || !oldContent.applicableRoles().containsAll(newContent.applicableRoles());
  }

  private boolean isBound(String metalake, String policyName) {
    RelationalEntity<?>[] tags =
        GravitinoEnv.getInstance()
            .policyDispatcher()
            .listTagAssociationsForPolicy(metalake, policyName);
    for (RelationalEntity<?> tag : tags) {
      String tagName = tag.targetEntity().name();
      if (!HiddenPolicyTag.isReservedName(tagName)
          || GravitinoEnv.getInstance()
                  .tagDispatcher()
                  .listMetadataObjectsForTag(metalake, tagName)
                  .length
              > 0) {
        return true;
      }
    }
    return false;
  }
}
