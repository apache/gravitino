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
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.ParameterUtil;

/**
 * Authorization executor for load table operations.
 *
 * <p>This executor examines the `privileges` parameter from the client to determine authorization:
 * MODIFY_TABLE privilege triggers stricter authorization, otherwise SELECT_TABLE is used.
 *
 * <p><b>Security Limitation:</b> This is a trust-based model. The client declares intended
 * privileges, and the server trusts this declaration without validation. A malicious or modified
 * client could request only SELECT_TABLE privileges to bypass MODIFY_TABLE authorization checks.
 * Legacy clients without the `privileges` parameter will use default authorization.
 */
public class LoadTableAuthorizationExecutor extends CommonAuthorizerExecutor {
  public LoadTableAuthorizationExecutor(
      Parameter[] parameters,
      Object[] args,
      String expression,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      Map<String, Object> pathParams,
      Optional<String> entityType) {
    super(expression, metadataContext, pathParams, entityType);

    String privileges = (String) ParameterUtil.extractFromParameters(parameters, args);

    if (privileges != null) {
      Set<Privilege.Name> privilegeNames =
          Arrays.stream(privileges.split(","))
              .map(Privilege.Name::valueOf)
              .collect(Collectors.toSet());

      if (privilegeNames.contains(Privilege.Name.MODIFY_TABLE)) {
        this.expression = AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION;
        this.authorizationExpressionEvaluator =
            new AuthorizationExpressionEvaluator(
                AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION);
      }
    }
  }
}
