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
import java.util.Optional;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
 * metadata authorization.
 */
@SuppressWarnings("FormatStringAnnotation")
public abstract class BaseMetadataAuthorizationMethodInterceptor implements MethodInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseMetadataAuthorizationMethodInterceptor.class);

  /**
   * Handler for request-specific authorization processing that cannot be handled by standard
   * annotation-based expressions. Implementations can enrich identifiers, validate requests, and/or
   * perform custom authorization.
   */
  protected interface AuthorizationHandler {
    /**
     * Process the request for authorization purposes. This may include:
     *
     * <ul>
     *   <li>Extracting additional identifiers from request bodies
     *   <li>Validating request parameters
     *   <li>Performing custom authorization logic
     * </ul>
     *
     * @param nameIdentifierMap Name identifier map (can be modified to add identifiers)
     * @throws ForbiddenException if authorization or validation fails
     */
    void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap)
        throws ForbiddenException;

    /**
     * Whether this handler has completed full authorization. Called after {@link #process} to
     * determine if standard expression-based authorization should be skipped.
     *
     * @return true if authorization is complete (skip standard check), false to continue with
     *     standard expression-based authorization
     */
    boolean authorizationCompleted();
  }

  protected abstract Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args);

  /**
   * Create an authorization handler for this request, if special handling is needed beyond standard
   * annotation-based authorization.
   *
   * <p>Override this method to provide custom handlers based on request characteristics (e.g.,
   * annotations, request types, parameters).
   *
   * @param parameters Method parameters
   * @param args Method arguments
   * @return Optional handler for custom authorization processing, or empty if standard
   *     authorization is sufficient
   */
  protected Optional<AuthorizationHandler> createAuthorizationHandler(
      Parameter[] parameters, Object[] args) {
    return Optional.empty();
  }

  protected boolean isExceptionPropagate(Exception e) {
    return false;
  }

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
        Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
            extractNameIdentifierFromParameters(parameters, args);

        // Check if current user exists in the metalake.
        NameIdentifier metalakeIdent = nameIdentifierMap.get(Entity.EntityType.METALAKE);

        if (metalakeIdent != null) {
          String currentUser = PrincipalUtils.getCurrentUserName();
          try {
            AuthorizationUtils.checkCurrentUser(metalakeIdent.name(), currentUser);
          } catch (org.apache.gravitino.exceptions.ForbiddenException ex) {
            LOG.info(
                "User validation failed - User: '{}', Metalake: '{}', Reason: {}",
                currentUser,
                metalakeIdent.name(),
                ex.getMessage());
            return IcebergExceptionMapper.toRESTResponse(ex);
          } catch (Exception ex) {
            LOG.error(
                "Unexpected error during user validation - User: '{}', Metalake: '{}'",
                currentUser,
                metalakeIdent.name(),
                ex);
            return IcebergExceptionMapper.toRESTResponse(
                new RuntimeException("Failed to validate user", ex));
          }
        }

        // Process custom authorization if handler exists
        Optional<AuthorizationHandler> handler = createAuthorizationHandler(parameters, args);
        boolean skipStandardCheck = false;

        if (handler.isPresent()) {
          AuthorizationHandler authzHandler = handler.get();
          authzHandler.process(nameIdentifierMap);
          skipStandardCheck = authzHandler.authorizationCompleted();
        }

        // Perform standard authorization check if custom handler didn't complete it
        if (!skipStandardCheck) {
          Map<String, Object> pathParams = Utils.extractPathParamsFromParameters(parameters, args);
          AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
              new AuthorizationExpressionEvaluator(expression);
          boolean authorizeResult =
              authorizationExpressionEvaluator.evaluate(
                  nameIdentifierMap,
                  pathParams,
                  new AuthorizationRequestContext(),
                  Optional.empty());
          if (!authorizeResult) {
            MetadataObject.Type type = expressionAnnotation.accessMetadataType();
            NameIdentifier accessMetadataName =
                nameIdentifierMap.get(Entity.EntityType.valueOf(type.name()));
            String currentUser = PrincipalUtils.getCurrentUserName();
            String methodName = method.getName();
            String notAuthzMessage =
                String.format(
                    "User '%s' is not authorized to perform operation '%s' on metadata '%s' with expression '%s'",
                    currentUser, methodName, accessMetadataName, expression);
            LOG.info(notAuthzMessage);
            return IcebergExceptionMapper.toRESTResponse(new ForbiddenException(notAuthzMessage));
          }
        }
      }
    } catch (Exception ex) {
      if (isExceptionPropagate(ex)) {
        return IcebergExceptionMapper.toRESTResponse(ex);
      }
      String currentUser = PrincipalUtils.getCurrentUserName();
      String methodName = methodInvocation.getMethod().getName();

      String errorMessage =
          String.format(
              "Authorization failed due to system internal error, User: '%s', Operation: '%s'",
              currentUser, methodName);
      LOG.info(errorMessage, ex);
      return IcebergExceptionMapper.toRESTResponse(new RuntimeException(errorMessage, ex));
    }
    try {
      return methodInvocation.proceed();
    } catch (Throwable e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }
}
