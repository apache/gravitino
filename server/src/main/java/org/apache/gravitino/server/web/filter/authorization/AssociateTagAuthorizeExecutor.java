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

import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;

public class AssociateTagAuthorizeExecutor implements AuthorizeExecutor {

  private final Parameter[] parameters;
  private final Object[] args;
  private final Map<Entity.EntityType, NameIdentifier> metadataContext;
  private final AuthorizationExpressionEvaluator authorizationExpressionEvaluator;
  private final Map<String, Object> pathParams;
  private final String entityType;

  public AssociateTagAuthorizeExecutor(
      Parameter[] parameters,
      Object[] args,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      AuthorizationExpressionEvaluator authorizationExpressionEvaluator,
      Map<String, Object> pathParams,
      String entityType) {
    this.parameters = parameters;
    this.args = args;
    this.metadataContext = metadataContext;
    this.authorizationExpressionEvaluator = authorizationExpressionEvaluator;
    this.pathParams = pathParams;
    this.entityType = entityType;
  }

  @Override
  public boolean execute() throws Exception {
    Object request = extractFromParameters(parameters, args);
    if (request == null) {
      return false;
    }

    AuthorizationRequestContext context = new AuthorizationRequestContext();
    Entity.EntityType targetType =
        Entity.EntityType.TAG; // Tags are the only supported batch target here

    // Authorize both 'tagsToAdd' and 'tagsToRemove' fields.
    // Short-circuit on first failure.
    return authorizeTagField(request, "tagsToAdd", context, targetType)
        && authorizeTagField(request, "tagsToRemove", context, targetType);
  }

  /**
   * Performs batch authorization for a given field (e.g., "tagsToAdd" or "tagsToRemove") containing
   * an array of tag names.
   *
   * @param request The request object containing the field.
   * @param fieldName The name of the field to reflect and read (must be a String[]).
   * @param context The shared authorization request context.
   * @param targetType The entity type being authorized (expected to be TAG).
   * @return {@code true} if all tags in the field pass authorization; {@code false} otherwise.
   * @throws IllegalAccessException if the field is not accessible.
   */
  private boolean authorizeTagField(
      Object request,
      String fieldName,
      AuthorizationRequestContext context,
      Entity.EntityType targetType)
      throws IllegalAccessException, NoSuchFieldException {

    Field field = request.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    String[] tagNames = (String[]) field.get(request);

    // Treat null or empty arrays as no-op (implicitly authorized)
    if (tagNames == null) {
      return true;
    }

    for (String tagName : tagNames) {
      // Skip null entries defensively
      if (tagName == null) {
        continue;
      }

      // Use a fresh context copy for each tag to avoid cross-contamination
      Map<Entity.EntityType, NameIdentifier> currentContext = new HashMap<>(this.metadataContext);
      buildNameIdentifierForBatchAuthorization(currentContext, tagName, targetType);

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
