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

import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.ParameterUtil;

/**
 * Authorization executor for testing the connection of an existing catalog.
 *
 * <p>Testing with the stored configuration uses the default expression. When the request carries
 * proposed changes and the condition is {@link ExpressionCondition#HAS_PROPOSED_CHANGES}, the
 * secondary expression is used instead, because the caller chooses the configuration the server
 * connects to.
 */
public class CatalogConnectionTestAuthorizationExecutor extends CommonAuthorizerExecutor {

  /**
   * Creates an authorization executor for an existing catalog connection test.
   *
   * @param parameters the parameters of the intercepted method
   * @param args the arguments passed to the intercepted method
   * @param expression the expression for testing with the stored configuration
   * @param metadataContext the metadata context bound to the authorization expression
   * @param pathParams the path parameters of the request
   * @param entityType the optional entity type of the request
   * @param secondaryExpression the expression for testing with proposed changes
   * @param secondaryExpressionCondition the condition for using the secondary expression
   */
  public CatalogConnectionTestAuthorizationExecutor(
      Parameter[] parameters,
      Object[] args,
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      String secondaryExpression,
      ExpressionCondition secondaryExpressionCondition) {
    super(expression, metadataContext, pathParams, entityType);
    if (StringUtils.isBlank(secondaryExpression)
        || secondaryExpressionCondition != ExpressionCondition.HAS_PROPOSED_CHANGES) {
      return;
    }

    // An empty change list is the same as testing the stored configuration.
    Object request = ParameterUtil.extractFromParameters(parameters, args);
    if (request instanceof CatalogUpdatesRequest updatesRequest
        && updatesRequest.getUpdates() != null
        && !updatesRequest.getUpdates().isEmpty()) {
      this.expression = secondaryExpression;
      this.authorizationExpressionEvaluator =
          new AuthorizationExpressionEvaluator(secondaryExpression);
    }
  }
}
