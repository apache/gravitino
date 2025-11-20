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
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

public class CommonAuthorizerExecutor implements AuthorizationExecutor {

  private Map<Entity.EntityType, NameIdentifier> metadataContext;
  private AuthorizationExpressionEvaluator authorizationExpressionEvaluator;
  private Map<String, Object> pathParams;
  String entityType;

  public CommonAuthorizerExecutor(
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      AuthorizationExpressionEvaluator authorizationExpressionEvaluator,
      Map<String, Object> pathParams,
      String entityType) {
    this.metadataContext = metadataContext;
    this.authorizationExpressionEvaluator = authorizationExpressionEvaluator;
    this.pathParams = pathParams;
    this.entityType = entityType;
  }

  @Override
  public boolean execute() {
    return authorizationExpressionEvaluator.evaluate(
        metadataContext,
        pathParams,
        new AuthorizationRequestContext(),
        Optional.ofNullable(entityType));
  }
}
