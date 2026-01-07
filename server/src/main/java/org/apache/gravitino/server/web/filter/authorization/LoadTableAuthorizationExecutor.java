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
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.ParameterUtil;

/**
 * Authorization executor for load table operations.
 *
 * <p>This executor uses secondaryExpression and secondaryExpressionCondition from the annotation to
 * determine authorization: if the condition is met (e.g., client requests MODIFY_TABLE privilege),
 * the secondaryExpression is used for stricter authorization, otherwise the default expression is
 * used (e.g., SELECT_TABLE).
 */
public class LoadTableAuthorizationExecutor extends CommonAuthorizerExecutor {
  public LoadTableAuthorizationExecutor(
      Parameter[] parameters,
      Object[] args,
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType,
      String secondaryExpression,
      String secondaryExpressionCondition) {
    super(expression, metadataContext, pathParams, entityType);

    // If secondaryExpression and condition are provided, evaluate the condition
    if (StringUtils.isNotBlank(secondaryExpression)
        && StringUtils.isNotBlank(secondaryExpressionCondition)) {
      String privileges = (String) ParameterUtil.extractFromParameters(parameters, args);

      // Evaluate the condition: does the request contain MODIFY_TABLE privilege?
      if (privileges != null
          && secondaryExpressionCondition.equals(
              AuthorizationExpressionConstants.REQUEST_REQUIRED_PRIVILEGES_CONTAINS_MODIFY_TABLE)) {
        Set<Privilege.Name> privilegeNames =
            Arrays.stream(privileges.split(","))
                .map(Privilege.Name::valueOf)
                .collect(Collectors.toSet());

        if (privilegeNames.contains(Privilege.Name.MODIFY_TABLE)) {
          // Use the secondary expression for stricter authorization
          this.expression = secondaryExpression;
          this.authorizationExpressionEvaluator =
              new AuthorizationExpressionEvaluator(secondaryExpression);
        }
      }
    }
  }
}
