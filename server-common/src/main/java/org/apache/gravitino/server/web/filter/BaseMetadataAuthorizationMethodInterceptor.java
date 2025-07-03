/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.Utils;

/**
 * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
 * metadata authorization.
 */
public abstract class BaseMetadataAuthorizationMethodInterceptor implements MethodInterceptor {

  abstract Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args);

  /**
   * Determine whether authorization is required and the rules via the authorization annotation ,
   * and obtain the metadata ID that requires authorization via the authorization annotation.
   *
   * @param methodInvocation methodInvocation with the Method object
   * @return the return result of the original method.
   * @throws Throwable throw an exception when authorization fails.
   */
  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    try {
      Method method = methodInvocation.getMethod();
      Parameter[] parameters = method.getParameters();
      AuthorizationExpression expressionAnnotation =
          method.getAnnotation(AuthorizationExpression.class);
      if (expressionAnnotation != null) {
        String expression = expressionAnnotation.expression();
        Object[] args = methodInvocation.getArguments();
        Map<EntityType, NameIdentifier> metadataContext =
            extractNameIdentifierFromParameters(parameters, args);
        AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
            new AuthorizationExpressionEvaluator(expression);

        boolean authorizeResult = authorizationExpressionEvaluator.evaluate(metadataContext);
        if (!authorizeResult) {
          MetadataObject.Type type = expressionAnnotation.accessMetadataType();
          NameIdentifier accessMetadataName =
              metadataContext.get(Entity.EntityType.valueOf(type.name()));
          String errorMessage = expressionAnnotation.errorMessage();
          return buildNoAuthResponse(errorMessage, accessMetadataName);
        }
      }
      return methodInvocation.proceed();
    } catch (Exception ex) {
      return Utils.forbidden("Can not access metadata. Cause by: " + ex.getMessage(), ex);
    }
  }

  private Response buildNoAuthResponse(String errorMessage, NameIdentifier accessMetadataName) {
    if (StringUtils.isNotBlank(errorMessage)) {
      return Utils.forbidden(
          errorMessage,
          new ForbiddenException("Can not access metadata, cause by: %s", errorMessage));
    }
    return Utils.forbidden(
        String.format("Can not access metadata {%s}.", accessMetadataName.name()),
        new ForbiddenException("Can not access metadata {%s}.", accessMetadataName));
  }
}
