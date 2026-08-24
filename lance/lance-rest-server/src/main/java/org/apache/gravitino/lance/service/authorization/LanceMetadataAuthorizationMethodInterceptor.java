/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

import com.google.common.annotations.VisibleForTesting;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.common.ops.gravitino.ObjectIdentifier;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.lance.service.authorization.annotations.LanceAuthorizationExpression;
import org.apache.gravitino.lance.service.authorization.annotations.LanceNamespaceDelimiter;
import org.apache.gravitino.lance.service.authorization.annotations.LanceNamespaceId;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.PermissionDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorizes Lance REST metadata operations before they run.
 *
 * <p>The interceptor decodes the Lance namespace ID of the request, resolves it to the Gravitino
 * entities it addresses and evaluates the authorization expression declared by {@link
 * LanceAuthorizationExpression}. An unauthorized request is answered with a Lance error response
 * and never reaches the operation.
 */
public class LanceMetadataAuthorizationMethodInterceptor implements MethodInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(LanceMetadataAuthorizationMethodInterceptor.class);

  private static final int SCHEMA_NAMESPACE_LEVELS = 2;

  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    Method method = methodInvocation.getMethod();
    LanceAuthorizationExpression annotation =
        method.getAnnotation(LanceAuthorizationExpression.class);
    if (annotation == null || !LanceRESTServerContext.getInstance().isAuthorizationEnabled()) {
      return methodInvocation.proceed();
    }

    Parameter[] parameters = method.getParameters();
    Object[] args = methodInvocation.getArguments();
    // A method without a namespace ID parameter, such as listing on the root, always addresses the
    // root namespace.
    String namespaceId = argumentAnnotatedWith(parameters, args, LanceNamespaceId.class).orElse("");
    String delimiter =
        argumentAnnotatedWith(parameters, args, LanceNamespaceDelimiter.class)
            .orElse(NamespaceWrapper.NAMESPACE_DELIMITER_DEFAULT);

    Optional<Object> denial;
    try {
      denial = authorize(annotation, method.getName(), namespaceId, delimiter);
    } catch (Exception e) {
      LOG.error(
          "Failed to authorize Lance operation '{}' on namespace '{}'",
          method.getName(),
          namespaceId,
          e);
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }

    return denial.isPresent() ? denial.get() : methodInvocation.proceed();
  }

  /**
   * Authorizes a single request.
   *
   * @return the response to send back when the request is rejected, empty when it is authorized.
   */
  private Optional<Object> authorize(
      LanceAuthorizationExpression annotation,
      String operation,
      String namespaceId,
      String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    if (nsId.levels() > SCHEMA_NAMESPACE_LEVELS
        || (nsId.levels() == 0 && !annotation.allowRootNamespace())) {
      return Optional.of(
          toResponse(
              namespaceId,
              new InvalidInputException(
                  "Unsupported Lance namespace identifier: " + namespaceId, "", namespaceId)));
    }

    String metalakeName = LanceRESTServerContext.getInstance().metalakeName();
    String currentUser = PrincipalUtils.getCurrentUserName();
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    Optional<Object> denial =
        checkUserAndActiveRoles(metalakeName, currentUser, namespaceId, requestContext);
    if (denial.isPresent()) {
      return denial;
    }

    // The root namespace holds no privileges of its own. Every valid user of the metalake may list
    // it, and the catalogs it returns are filtered afterwards.
    if (nsId.levels() == 0) {
      return Optional.empty();
    }

    String catalogName = nsId.levelAtListPos(0);
    Map<Entity.EntityType, NameIdentifier> nameIdentifiers = new HashMap<>();
    nameIdentifiers.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    nameIdentifiers.put(
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalogName));
    String expression = annotation.catalogExpression();
    if (nsId.levels() == SCHEMA_NAMESPACE_LEVELS) {
      nameIdentifiers.put(
          Entity.EntityType.SCHEMA,
          NameIdentifierUtil.ofSchema(metalakeName, catalogName, nsId.levelAtListPos(1)));
      expression = annotation.schemaExpression();
    }

    if (evaluateExpression(expression, nameIdentifiers, requestContext)) {
      return Optional.empty();
    }

    String message =
        String.format(
            "User '%s' is not authorized to perform operation '%s' on Lance namespace '%s'",
            currentUser, operation, namespaceId);
    LOG.info("{}, expression: {}", message, expression);
    return Optional.of(
        toResponse(namespaceId, new PermissionDeniedException(message, "", namespaceId)));
  }

  /**
   * Evaluates one authorization expression against the entities the request addresses.
   *
   * @param expression the authorization expression to evaluate.
   * @param nameIdentifiers the entities the request addresses.
   * @param requestContext the context shared by the checks of this request.
   * @return {@code true} when the caller is authorized.
   */
  @VisibleForTesting
  boolean evaluateExpression(
      String expression,
      Map<Entity.EntityType, NameIdentifier> nameIdentifiers,
      AuthorizationRequestContext requestContext) {
    return new AuthorizationExpressionEvaluator(expression)
        .evaluate(nameIdentifiers, requestContext);
  }

  /**
   * Rejects a user that does not belong to the metalake, and a caller that declares active roles it
   * does not hold.
   */
  private Optional<Object> checkUserAndActiveRoles(
      String metalakeName,
      String currentUser,
      String namespaceId,
      AuthorizationRequestContext requestContext) {
    try {
      AuthorizationUtils.checkCurrentUser(metalakeName, currentUser, requestContext);
    } catch (org.apache.gravitino.exceptions.ForbiddenException e) {
      LOG.info(
          "User validation failed - user: '{}', metalake: '{}', reason: {}",
          currentUser,
          metalakeName,
          e.getMessage());
      return Optional.of(
          toResponse(
              namespaceId,
              new PermissionDeniedException(e.getMessage(), getStackTrace(e), namespaceId)));
    }

    ActiveRoles activeRoles = requestContext.getActiveRoles();
    if (activeRoles.mode() != ActiveRoles.Mode.NAMED) {
      return Optional.empty();
    }

    Set<String> unheldRoles =
        GravitinoAuthorizerProvider.getInstance()
            .getGravitinoAuthorizer()
            .findUnheldRoles(
                PrincipalUtils.getCurrentPrincipal(),
                metalakeName,
                activeRoles.roleNames(),
                requestContext);
    if (unheldRoles.isEmpty()) {
      return Optional.empty();
    }

    String message =
        String.format(
            "User '%s' cannot assume active role(s) that are not held: %s",
            currentUser, unheldRoles);
    LOG.info(message);
    return Optional.of(
        toResponse(namespaceId, new PermissionDeniedException(message, "", namespaceId)));
  }

  private Object toResponse(String namespaceId, Exception e) {
    return LanceExceptionMapper.toRESTResponse(namespaceId, e);
  }

  private Optional<String> argumentAnnotatedWith(
      Parameter[] parameters, Object[] args, Class<? extends Annotation> annotationType) {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i].getAnnotation(annotationType) != null && args[i] != null) {
        return Optional.of(String.valueOf(args[i]));
      }
    }
    return Optional.empty();
  }
}
