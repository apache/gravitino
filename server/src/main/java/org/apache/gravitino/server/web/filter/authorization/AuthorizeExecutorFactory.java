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
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

public class AuthorizeExecutorFactory {

  public static AuthorizationExecutor create(
      String expression,
      AuthorizationRequest.RequestType requestType,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      AuthorizationExpressionEvaluator authorizationExpressionEvaluator,
      Map<String, Object> pathParams,
      String entityType,
      Parameter[] parameters,
      Object[] args) {
    return switch (requestType) {
      case COMMON -> new CommonAuthorizerExecutor(
          expression, metadataContext, authorizationExpressionEvaluator, pathParams, entityType);
      case ASSOCIATE_TAG -> new AssociateTagAuthorizationExecutor(
          expression,
          parameters,
          args,
          metadataContext,
          authorizationExpressionEvaluator,
          pathParams,
          entityType);
      case ASSOCIATE_POLICY -> new AssociatePolicyAuthorizationExecutor(
          expression,
          parameters,
          args,
          metadataContext,
          authorizationExpressionEvaluator,
          pathParams,
          entityType);
      case RUN_JOB -> new RunJobAuthorizationExecutor(
          parameters,
          args,
          metadataContext,
          authorizationExpressionEvaluator,
          pathParams,
          entityType);
    };
  }
}
