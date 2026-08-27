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

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.common.ops.gravitino.CommonUtil;
import org.apache.gravitino.lance.common.ops.gravitino.ObjectIdentifier;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.lance.service.authorization.annotations.LanceRootNamespace;
import org.apache.gravitino.lance.service.rest.LanceTableOperations;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.PermissionDeniedException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.RegisterTableRequest;

/** Resolves Lance namespace IDs and maps shared authorization failures to Lance REST responses. */
public class LanceMetadataAuthorizationMethodInterceptor
    extends BaseMetadataAuthorizationMethodInterceptor implements MethodInterceptor {

  private static final int CATALOG_NAMESPACE_LEVELS = 1;
  private static final int SCHEMA_NAMESPACE_LEVELS = 2;
  private static final int TABLE_IDENTIFIER_LEVELS = 3;

  private final String metalakeName;

  /**
   * Creates an interceptor for the metalake exposed by this Lance REST service.
   *
   * @param metalakeName metalake exposed by the service
   */
  public LanceMetadataAuthorizationMethodInterceptor(String metalakeName) {
    this.metalakeName = metalakeName;
  }

  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    return authorizeMethod(
        methodInvocation.getMethod(), methodInvocation.getArguments(), methodInvocation::proceed);
  }

  @Override
  protected AuthorizationTarget resolveAuthorizationTarget(
      Method method, AuthorizationExpression annotation, Parameter[] parameters, Object[] args) {
    Optional<String> namespaceId = pathArgument(parameters, args, "id");
    boolean rootNamespace = method.isAnnotationPresent(LanceRootNamespace.class);
    if (rootNamespace) {
      if (namespaceId.isPresent()) {
        throw new IllegalStateException(
            "A Lance root operation must not declare a namespace ID target");
      }
      return new AuthorizationTarget(baseIdentifiers(), Entity.EntityType.METALAKE);
    }

    // An absent ID is a programming error, not the root namespace. Root operations must opt in
    // explicitly so a forgotten annotation cannot silently weaken authorization.
    String targetId =
        namespaceId.orElseThrow(
            () ->
                new IllegalStateException(
                    "An authorized Lance operation must declare @PathParam(\"id\") or "
                        + "@LanceRootNamespace"));
    String delimiter =
        queryArgument(parameters, args, "delimiter")
            .orElse(NamespaceWrapper.NAMESPACE_DELIMITER_DEFAULT);
    ObjectIdentifier identifier = ObjectIdentifier.of(targetId, Pattern.quote(delimiter));
    if (identifier.levels() == 0 || identifier.levels() > maxIdentifierLevels(method)) {
      throw unsupportedIdentifier(targetId);
    }

    // A Lance identifier carries its depth rather than its kind, so the addressed entity type
    // follows from the level count: one level is a catalog, two a schema, three a table. An
    // identifier of the wrong depth for an operation matches no branch of that operation's
    // expression and is therefore denied rather than silently authorized against another type.
    Map<Entity.EntityType, NameIdentifier> identifiers = baseIdentifiers();
    String catalogName = identifier.levelAtListPos(0);
    identifiers.put(
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalogName));
    if (identifier.levels() == CATALOG_NAMESPACE_LEVELS) {
      return new AuthorizationTarget(identifiers, Entity.EntityType.CATALOG);
    }

    String schemaName = identifier.levelAtListPos(1);
    identifiers.put(
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema(metalakeName, catalogName, schemaName));
    if (identifier.levels() == SCHEMA_NAMESPACE_LEVELS) {
      return new AuthorizationTarget(identifiers, Entity.EntityType.SCHEMA);
    }

    identifiers.put(
        Entity.EntityType.TABLE,
        NameIdentifierUtil.ofTable(
            metalakeName, catalogName, schemaName, identifier.levelAtListPos(2)));
    return new AuthorizationTarget(identifiers, Entity.EntityType.TABLE);
  }

  /**
   * Returns the deepest identifier the given operation can address. Only the table resource accepts
   * a table identifier; every namespace operation stops at a schema, so a deeper identifier is
   * rejected as unsupported before any expression sees it. Anything else falls back to the
   * shallower bound, so a resource added later cannot widen its own reach by omission.
   *
   * @param method invoked protocol method
   * @return the maximum number of levels the operation's identifier may carry
   */
  private static int maxIdentifierLevels(Method method) {
    return LanceTableOperations.class.isAssignableFrom(method.getDeclaringClass())
        ? TABLE_IDENTIFIER_LEVELS
        : SCHEMA_NAMESPACE_LEVELS;
  }

  /**
   * Returns the handler that authorizes an overwrite of an existing object. The mode of a Lance
   * create request travels in the request body or in a query parameter rather than in the request
   * path, so which privileges a create needs cannot be expressed by the method annotation alone.
   *
   * @param method invoked protocol method
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   * @return the overwrite handler for a create request, empty for every other operation
   */
  @Override
  protected Optional<AuthorizationHandler> createAuthorizationHandler(
      Method method, Parameter[] parameters, Object[] args) {
    return createMode(parameters, args).map(OverwriteAuthzHandler::new);
  }

  @Override
  protected boolean shouldSkipExpressionEvaluation(AuthorizationTarget target) {
    // The root has no privilege-bearing Lance object. The shared pipeline still validates the
    // caller, while the metadata filter removes catalogs that the caller may not see.
    return target.entityType() == Entity.EntityType.METALAKE;
  }

  @Override
  protected boolean isExceptionPropagate(Exception exception) {
    return exception instanceof LanceNamespaceException;
  }

  @Override
  protected Object toErrorResponse(Method method, Object[] args, Throwable throwable) {
    String namespaceId = pathArgument(method.getParameters(), args, "id").orElse("");
    Exception exception;
    if (throwable instanceof ForbiddenException) {
      exception =
          new PermissionDeniedException(
              throwable.getMessage(), getStackTrace(throwable), namespaceId);
    } else if (throwable instanceof Exception) {
      exception = (Exception) throwable;
    } else {
      exception = new RuntimeException(throwable);
    }
    return LanceExceptionMapper.toRESTResponse(namespaceId, exception);
  }

  /**
   * Authorizes a create request whose mode overwrites an object that already exists.
   *
   * <p>An overwrite replaces an existing namespace or table, so it is a modification rather than a
   * creation and is authorized against the ownership expression for the addressed entity instead of
   * the create expression on the method. Without this, CREATE_CATALOG, CREATE_SCHEMA, or
   * CREATE_TABLE would escalate into permission to replace an object the caller does not own.
   *
   * <p>The mode alone decides this, without probing whether the object exists: an existence probe
   * at authorization time would race with the create that follows it, and the required privilege
   * would then depend on that race.
   */
  private static final class OverwriteAuthzHandler implements AuthorizationHandler {

    private static final String OVERWRITE_MODE = "OVERWRITE";

    private final boolean overwrite;

    private OverwriteAuthzHandler(String mode) {
      // Read the mode through the same normalization the create operation applies, so a token the
      // operation will act on as an overwrite cannot be authorized as a plain create. Comparing the
      // raw string here would leave a gap: " overwrite " reaches the operation as OVERWRITE but
      // would not match, and a caller holding only a create privilege could replace an object owned
      // by somebody else.
      this.overwrite = OVERWRITE_MODE.equals(CommonUtil.normalizeToken(mode));
    }

    @Override
    public void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
      if (!overwrite) {
        return;
      }

      Entity.EntityType entityType = deepestEntityType(nameIdentifierMap);
      String expression =
          entityType == Entity.EntityType.TABLE
              ? LanceAuthorizationExpressions.MODIFY_TABLE_AUTHORIZATION_EXPRESSION
              : LanceAuthorizationExpressions.MODIFY_NAMESPACE_AUTHORIZATION_EXPRESSION;
      boolean authorized =
          new AuthorizationExpressionEvaluator(expression)
              .evaluate(
                  nameIdentifierMap,
                  new HashMap<>(),
                  new AuthorizationRequestContext(),
                  Optional.of(entityType.name()));
      if (!authorized) {
        throw new ForbiddenException(
            "User '%s' is not authorized to overwrite '%s'",
            PrincipalUtils.getCurrentUserName(), nameIdentifierMap.get(entityType));
      }
    }

    @Override
    public boolean authorizationCompleted() {
      // An overwrite is fully authorized here, so the create expression on the method must not be
      // evaluated afterwards. A create that does not overwrite falls through to it unchanged.
      return overwrite;
    }

    private static Entity.EntityType deepestEntityType(
        Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
      if (nameIdentifierMap.containsKey(Entity.EntityType.TABLE)) {
        return Entity.EntityType.TABLE;
      }
      return nameIdentifierMap.containsKey(Entity.EntityType.SCHEMA)
          ? Entity.EntityType.SCHEMA
          : Entity.EntityType.CATALOG;
    }
  }

  private Map<Entity.EntityType, NameIdentifier> baseIdentifiers() {
    Map<Entity.EntityType, NameIdentifier> identifiers = new HashMap<>();
    identifiers.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    return identifiers;
  }

  private InvalidInputException unsupportedIdentifier(String namespaceId) {
    return new InvalidInputException(
        "Unsupported Lance namespace identifier: " + namespaceId, "", namespaceId);
  }

  /**
   * Returns the requested create mode, or empty when the invoked operation does not create an
   * object. Lance carries the mode in the create-namespace body, in the register-table body, or in
   * the {@code mode} query parameter of create-table.
   */
  private static Optional<String> createMode(Parameter[] parameters, Object[] args) {
    for (Object arg : args) {
      if (arg instanceof CreateNamespaceRequest) {
        return Optional.of(Objects.toString(((CreateNamespaceRequest) arg).getMode(), ""));
      }
      if (arg instanceof RegisterTableRequest) {
        return Optional.of(Objects.toString(((RegisterTableRequest) arg).getMode(), ""));
      }
    }
    return queryArgument(parameters, args, "mode");
  }

  private static Optional<String> pathArgument(Parameter[] parameters, Object[] args, String name) {
    for (int i = 0; i < parameters.length; i++) {
      PathParam annotation = parameters[i].getAnnotation(PathParam.class);
      if (args[i] != null && annotation != null && name.equals(annotation.value())) {
        return Optional.of(String.valueOf(args[i]));
      }
    }
    return Optional.empty();
  }

  private static Optional<String> queryArgument(
      Parameter[] parameters, Object[] args, String name) {
    for (int i = 0; i < parameters.length; i++) {
      QueryParam annotation = parameters[i].getAnnotation(QueryParam.class);
      if (args[i] != null && annotation != null && name.equals(annotation.value())) {
        return Optional.of(String.valueOf(args[i]));
      }
    }
    return Optional.empty();
  }
}
