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
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.annotations.ExpressionAction;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.ParameterUtil;

/**
 * Authorization executor for load table operations.
 *
 * <p>This executor applies a three-expression authorization strategy driven by the conditions and
 * actions declared in {@link
 * org.apache.gravitino.server.authorization.annotations.AuthorizationExpression}:
 *
 * <ol>
 *   <li><b>Primary expression</b>: evaluated when {@code firstExpressionCondition} is satisfied —
 *       typically {@link ExpressionCondition#NOT_CONTAIN_REQUIRED_PRIVILEGES}, meaning the request
 *       carries no {@code ?privileges=} parameter.
 *   <li><b>Secondary expression</b>: evaluated <em>instead of</em> the primary when {@link
 *       ExpressionCondition#CONTAIN_REQUIRED_PRIVILEGES} is met, i.e. the client provides any
 *       required privileges via the {@code ?privileges=} parameter.
 *   <li><b>Third expression</b>: evaluated when {@link
 *       ExpressionCondition#PREVIOUS_EXPRESSION_FORBIDDEN} is met (primary/secondary denied). If
 *       the third expression passes and the action is {@link
 *       ExpressionAction#CHECK_METADATA_OBJECT_EXISTS}, authorization succeeds so that callers can
 *       verify object existence (e.g. Spark's {@code tableExists()}) even without full read access.
 * </ol>
 *
 * <p><b>Security note:</b> The {@link ExpressionCondition#CONTAIN_REQUIRED_PRIVILEGES} condition
 * is trust-based — the server trusts the client's declaration of required privileges. A client
 * that omits the {@code privileges} parameter will fall back to the less-strict primary expression.
 */
public class LoadTableAuthorizationExecutor extends CommonAuthorizerExecutor {

  private final String thirdExpression;
  private final ExpressionCondition thirdExpressionCondition;
  private final ExpressionAction thirdExpressionAction;

  public LoadTableAuthorizationExecutor(
      Parameter[] parameters,
      Object[] args,
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      ExpressionCondition firstExpressionCondition,
      ExpressionAction firstExpressionAction,
      String secondaryExpression,
      ExpressionCondition secondaryExpressionCondition,
      ExpressionAction secondaryExpressionAction,
      String thirdExpression,
      ExpressionCondition thirdExpressionCondition,
      ExpressionAction thirdExpressionAction) {
    super(expression, metadataContext, pathParams, entityType);

    String privileges = (String) ParameterUtil.extractFromParameters(parameters, args);
    boolean hasPrivileges = StringUtils.isNotBlank(privileges);

    // Determine which expression to activate:
    //  - Secondary wins when CONTAIN_REQUIRED_PRIVILEGES is set and privileges are present.
    //  - Primary (ALWAYS or NOT_CONTAIN_REQUIRED_PRIVILEGES with no privileges) runs otherwise.
    //  - If firstExpressionCondition restricts the primary to no-privileges requests but the
    //    request carries privileges, neither expression applies → deny.
    if (StringUtils.isNotBlank(secondaryExpression)
        && secondaryExpressionCondition == ExpressionCondition.CONTAIN_REQUIRED_PRIVILEGES
        && hasPrivileges) {
      this.expression = secondaryExpression;
      this.authorizationExpressionEvaluator =
          new AuthorizationExpressionEvaluator(secondaryExpression);
    } else if (firstExpressionCondition == ExpressionCondition.NOT_CONTAIN_REQUIRED_PRIVILEGES
        && hasPrivileges) {
      // Primary is restricted to no-privileges requests but privileges are present:
      // neither primary nor secondary applies — deny.
      this.expression = "";
      this.authorizationExpressionEvaluator = null;
    }
    // firstExpressionCondition == ALWAYS (default): primary always runs — no change needed.

    this.thirdExpression = thirdExpression;
    this.thirdExpressionCondition = thirdExpressionCondition;
    this.thirdExpressionAction = thirdExpressionAction;
  }

  @Override
  public boolean execute() throws Exception {
    if (authorizationExpressionEvaluator == null) {
      return false;
    }
    boolean result = super.execute();

    if (!result
        && thirdExpressionCondition == ExpressionCondition.PREVIOUS_EXPRESSION_FORBIDDEN
        && StringUtils.isNotBlank(thirdExpression)) {
      AuthorizationExpressionEvaluator thirdEvaluator =
          new AuthorizationExpressionEvaluator(thirdExpression);
      boolean thirdResult =
          thirdEvaluator.evaluate(
              metadataContext, pathParams, new AuthorizationRequestContext(), entityType);

      if (thirdResult && thirdExpressionAction == ExpressionAction.CHECK_METADATA_OBJECT_EXISTS) {
        return true;
      }
    }

    return result;
  }
}
