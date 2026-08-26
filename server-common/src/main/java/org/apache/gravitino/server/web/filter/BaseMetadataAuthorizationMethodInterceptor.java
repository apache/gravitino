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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the metadata authorization steps shared by REST protocols.
 *
 * <p>Protocol implementations only resolve request parameters into an {@link AuthorizationTarget}
 * and map failures to their response format. This class consistently validates the user and active
 * roles, runs any request-specific handler, and evaluates the standard authorization expression.
 */
@SuppressWarnings("FormatStringAnnotation")
public abstract class BaseMetadataAuthorizationMethodInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseMetadataAuthorizationMethodInterceptor.class);

  /** The metadata identifiers and entity type resolved from one protocol request. */
  protected static class AuthorizationTarget {
    private final Map<Entity.EntityType, NameIdentifier> nameIdentifiers;
    private final Entity.EntityType entityType;

    /**
     * Creates an authorization target.
     *
     * @param nameIdentifiers identifiers needed by the authorization expression
     * @param entityType the entity type directly addressed by this request
     */
    public AuthorizationTarget(
        Map<Entity.EntityType, NameIdentifier> nameIdentifiers, Entity.EntityType entityType) {
      this.nameIdentifiers = Objects.requireNonNull(nameIdentifiers, "nameIdentifiers");
      this.entityType = Objects.requireNonNull(entityType, "entityType");
    }

    /**
     * Returns the identifiers needed by the authorization expression. The map remains mutable so a
     * request-specific handler can add an identifier found in a request body.
     *
     * @return the identifiers keyed by entity type
     */
    public Map<Entity.EntityType, NameIdentifier> nameIdentifiers() {
      return nameIdentifiers;
    }

    /**
     * Returns the entity type directly addressed by this request.
     *
     * @return the target entity type
     */
    public Entity.EntityType entityType() {
      return entityType;
    }
  }

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
     * @throws Exception if authorization or validation fails
     */
    void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) throws Exception;

    /**
     * Whether this handler has completed full authorization. Called after {@link #process} to
     * determine if standard expression-based authorization should be skipped.
     *
     * @return true if authorization is complete (skip standard check), false to continue with
     *     standard expression-based authorization
     */
    boolean authorizationCompleted();
  }

  /** Invokes the protocol operation after authorization succeeds. */
  @FunctionalInterface
  protected interface MethodInvoker {
    /**
     * Invokes the intercepted protocol operation.
     *
     * @return the operation result
     * @throws Throwable if the operation fails
     */
    Object proceed() throws Throwable;
  }

  /**
   * Resolves the metadata identifiers and the directly addressed entity type from a protocol
   * request. The entity type is kept separately because some protocols encode catalog and schema
   * requests in the same path parameter.
   *
   * @param method invoked protocol method
   * @param annotation authorization annotation on the invoked method
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   * @return the resolved authorization target
   */
  protected abstract AuthorizationTarget resolveAuthorizationTarget(
      Method method, AuthorizationExpression annotation, Parameter[] parameters, Object[] args);

  /**
   * Maps an authorization or operation failure to the response format required by a protocol.
   *
   * @param method invoked protocol method
   * @param args invoked method arguments
   * @param throwable failure to map
   * @return the protocol response
   */
  protected abstract Object toErrorResponse(Method method, Object[] args, Throwable throwable);

  /**
   * Create an authorization handler for this request, if special handling is needed beyond standard
   * annotation-based authorization.
   *
   * <p>Override this method to provide custom handlers based on request characteristics (e.g.,
   * annotations, request types, parameters).
   *
   * @param method REST method being invoked
   * @param parameters Method parameters
   * @param args Method arguments
   * @return Optional handler for custom authorization processing, or empty if standard
   *     authorization is sufficient
   */
  protected Optional<AuthorizationHandler> createAuthorizationHandler(
      Method method, Parameter[] parameters, Object[] args) {
    return Optional.empty();
  }

  /**
   * Returns whether an exception should be returned as a protocol error without being wrapped as an
   * internal authorization failure.
   *
   * @param exception exception raised while authorizing the request
   * @return {@code true} to preserve the original exception
   */
  protected boolean isExceptionPropagate(Exception exception) {
    return false;
  }

  /**
   * Returns whether the complete local authorization pipeline should be skipped. This is intended
   * for a protocol proxy whose downstream Gravitino server authorizes the same request.
   *
   * @param nameIdentifierMap identifiers resolved from the request
   * @return {@code true} to skip user validation, handlers, and expression evaluation
   */
  protected boolean shouldSkipAuthorization(
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
    return false;
  }

  /**
   * Hook for requests that must validate the current user and active roles but do not have a
   * metadata object on which to evaluate an expression. A protocol root-list request is a typical
   * example: its returned children are filtered separately.
   *
   * @param target resolved authorization target
   * @return {@code true} to skip only expression evaluation
   */
  protected boolean shouldSkipExpressionEvaluation(AuthorizationTarget target) {
    return false;
  }

  /**
   * Authorizes a protocol method and maps all failures through the protocol hook.
   *
   * <p>The protocol-specific interceptor adapts its invocation framework to these plain Java
   * arguments. Keeping that adapter outside this shared pipeline avoids coupling server-common to
   * the interception framework used by each REST server.
   *
   * @param method method to authorize
   * @param args method arguments
   * @param methodInvoker operation to invoke after authorization succeeds
   * @return the mapped error response, or the result of the invoked method
   * @throws Throwable if the invocation infrastructure itself cannot run
   */
  protected final Object authorizeMethod(Method method, Object[] args, MethodInvoker methodInvoker)
      throws Throwable {
    try {
      Parameter[] parameters = method.getParameters();
      AuthorizationExpression expressionAnnotation =
          method.getAnnotation(AuthorizationExpression.class);
      if (expressionAnnotation != null) {
        String expression = expressionAnnotation.expression();
        AuthorizationTarget target =
            resolveAuthorizationTarget(method, expressionAnnotation, parameters, args);
        Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = target.nameIdentifiers();
        boolean skipStandardCheck = shouldSkipAuthorization(nameIdentifierMap);

        NameIdentifier metalakeIdent = nameIdentifierMap.get(Entity.EntityType.METALAKE);
        AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();

        if (!skipStandardCheck && metalakeIdent != null) {
          String currentUser = PrincipalUtils.getCurrentUserName();
          // Reuse this request context so user, role, and privilege checks see exactly the same
          // active-role selection and can share cached authorization results.
          try {
            AuthorizationUtils.checkCurrentUser(
                metalakeIdent.name(), currentUser, authorizationRequestContext);
          } catch (ForbiddenException exception) {
            LOG.info(
                "User validation failed - User: '{}', Metalake: '{}', Reason: {}",
                currentUser,
                metalakeIdent.name(),
                exception.getMessage());
            throw exception;
          } catch (Exception exception) {
            // User lookup failures are different from a missing user. Preserve the existing
            // protocol behavior by returning an internal error instead of a 403 denial.
            LOG.error(
                "Unexpected error during user validation - User: '{}', Metalake: '{}'",
                currentUser,
                metalakeIdent.name(),
                exception);
            return toErrorResponse(
                method, args, new RuntimeException("Failed to validate user", exception));
          }

          // ALL and NONE already describe a complete role selection. NAMED is different: every
          // requested role must be checked so a caller cannot assume somebody else's role.
          ActiveRoles activeRoles = authorizationRequestContext.getActiveRoles();
          if (activeRoles.mode() == ActiveRoles.Mode.NAMED) {
            Set<String> unheldRoles =
                GravitinoAuthorizerProvider.getInstance()
                    .getGravitinoAuthorizer()
                    .findUnheldRoles(
                        PrincipalUtils.getCurrentPrincipal(),
                        metalakeIdent.name(),
                        activeRoles.roleNames(),
                        authorizationRequestContext);
            if (!unheldRoles.isEmpty()) {
              String message =
                  String.format(
                      "User '%s' cannot assume active role(s) that are not held: %s",
                      currentUser, unheldRoles);
              LOG.info(message);
              throw new ForbiddenException(message);
            }
          }
        }

        Optional<AuthorizationHandler> handler =
            createAuthorizationHandler(method, parameters, args);

        if (!skipStandardCheck && handler.isPresent()) {
          AuthorizationHandler authzHandler = handler.get();
          authzHandler.process(nameIdentifierMap);
          skipStandardCheck = authzHandler.authorizationCompleted();
        }

        if (!skipStandardCheck && !shouldSkipExpressionEvaluation(target)) {
          Map<String, Object> pathParams = Utils.extractPathParamsFromParameters(parameters, args);
          AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
              new AuthorizationExpressionEvaluator(expression);
          boolean authorizeResult =
              authorizationExpressionEvaluator.evaluate(
                  nameIdentifierMap,
                  pathParams,
                  authorizationRequestContext,
                  Optional.of(target.entityType().name()));
          if (!authorizeResult) {
            NameIdentifier accessMetadataName = nameIdentifierMap.get(target.entityType());
            String currentUser = PrincipalUtils.getCurrentUserName();
            String methodName = method.getName();
            String notAuthzMessage =
                String.format(
                    "User '%s' is not authorized to perform operation '%s' on metadata '%s' with expression '%s'",
                    currentUser, methodName, accessMetadataName, expression);
            LOG.info(notAuthzMessage);
            throw new ForbiddenException(notAuthzMessage);
          }
        }
      }
    } catch (Exception ex) {
      if (ex instanceof ForbiddenException || isExceptionPropagate(ex)) {
        return toErrorResponse(method, args, ex);
      }
      String currentUser = PrincipalUtils.getCurrentUserName();
      String methodName = method.getName();

      String errorMessage =
          String.format(
              "Authorization failed due to system internal error, User: '%s', Operation: '%s'",
              currentUser, methodName);
      LOG.info(errorMessage, ex);
      return toErrorResponse(method, args, new RuntimeException(errorMessage, ex));
    }
    try {
      return methodInvoker.proceed();
    } catch (Throwable e) {
      return toErrorResponse(method, args, e);
    }
  }
}
