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
import org.apache.gravitino.GravitinoEnv;
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
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
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
    Method method = methodInvocation.getMethod();
    Object[] args = methodInvocation.getArguments();
    return authorizeMethod(
        method, args, () -> invokeWithExistOkRecheck(method, args, methodInvocation::proceed));
  }

  /**
   * Invokes the operation and re-authorizes an {@code exist_ok} result before it is returned.
   *
   * <p>The authorization that runs before the operation cannot know which object the operation will
   * resolve: a concurrent request may remove the addressed object and create a different one at the
   * same name between that check and the load the operation performs. This re-check runs after the
   * operation has resolved its result and before the result is returned, so the read privilege is
   * verified against the object the response is actually about to describe. A caller that just
   * created the object owns it and passes the check.
   *
   * @param method invoked protocol method
   * @param args invoked method arguments
   * @param invoker the intercepted operation
   * @return the operation result
   * @throws Throwable if the operation or the re-check fails
   */
  private Object invokeWithExistOkRecheck(Method method, Object[] args, MethodInvoker invoker)
      throws Throwable {
    Object result = invoker.proceed();
    Parameter[] parameters = method.getParameters();
    if (!isExistOkCreate(parameters, args)) {
      return result;
    }
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    if (annotation == null) {
      return result;
    }
    AuthorizationTarget target = resolveAuthorizationTarget(method, annotation, parameters, args);
    Map<Entity.EntityType, NameIdentifier> identifiers = target.nameIdentifiers();
    Entity.EntityType entityType = deepestEntityType(identifiers);
    if (objectExists(identifiers)
        && !evaluateExpression(readExpression(method), identifiers, entityType)) {
      throw new ForbiddenException(
          "User '%s' is not authorized to read existing metadata for '%s' in exist_ok mode",
          PrincipalUtils.getCurrentUserName(), identifiers.get(entityType));
    }
    return result;
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
    return isTableOperation(method) ? TABLE_IDENTIFIER_LEVELS : SCHEMA_NAMESPACE_LEVELS;
  }

  private static boolean isTableOperation(Method method) {
    return LanceTableOperations.class.isAssignableFrom(method.getDeclaringClass());
  }

  /**
   * Returns the handler that authorizes a create request whose mode decides what the request does
   * to an object that already exists. The mode of a Lance create request travels in the request
   * body or in a query parameter rather than in the request path, so which privileges a create
   * needs cannot be expressed by the method annotation alone.
   *
   * <p>An {@code exist_ok} request resolves to one of two outcomes the interceptor cannot tell
   * apart without resolving the addressed object: it answers with the metadata of an object that
   * exists, or it creates an object that does not. The handler resolves the addressed object first,
   * mirroring how the Gravitino core server authorizes a load that doubles as an existence probe: a
   * request that resolves to an existing object is authorized against the read expression of that
   * object, and a request that resolves to an absent one falls through to the create expression on
   * the method, which authorizes the creation that follows.
   *
   * @param method invoked protocol method
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   * @return the handler for a create request, empty for every other operation
   */
  @Override
  protected Optional<AuthorizationHandler> createAuthorizationHandler(
      Method method, Parameter[] parameters, Object[] args) {
    String overwriteExpression =
        isTableOperation(method)
            ? LanceAuthorizationExpressions.MODIFY_TABLE_AUTHORIZATION_EXPRESSION
            : LanceAuthorizationExpressions.MODIFY_NAMESPACE_AUTHORIZATION_EXPRESSION;
    return createMode(parameters, args)
        .map(mode -> new CreateModeAuthzHandler(mode, overwriteExpression, readExpression(method)));
  }

  /**
   * Returns the read expression matching the invoked resource. An {@code exist_ok} request that
   * resolves to an existing object answers with that object's metadata (location, properties,
   * schema), so the caller must hold the same read privilege a describe of the object would
   * require.
   *
   * @param method invoked protocol method
   * @return the read expression for the resource the method addresses
   */
  private static String readExpression(Method method) {
    return isTableOperation(method)
        ? LanceAuthorizationExpressions.READ_TABLE_AUTHORIZATION_EXPRESSION
        : AuthorizationExpressionConstants.CAN_ACCESS_METADATA;
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
      exception = new PermissionDeniedException(throwable.getMessage(), "", namespaceId);
    } else if (throwable instanceof Exception) {
      exception = (Exception) throwable;
    } else {
      exception = new RuntimeException(throwable);
    }
    return LanceExceptionMapper.toRESTResponse(namespaceId, exception);
  }

  /**
   * Authorizes a create request whose mode affects an existing object.
   *
   * <p>An overwrite replaces an existing namespace or table, so it is a modification rather than a
   * creation and is authorized against the modification expression for the invoked resource instead
   * of the create expression on the method. Without this, CREATE_CATALOG, CREATE_SCHEMA, or
   * CREATE_TABLE would escalate into permission to replace an object the caller does not own.
   *
   * <p>An {@code exist_ok} request has two outcomes the interceptor cannot tell apart without
   * resolving the addressed object. When the object exists, the request answers with the existing
   * object's metadata (location, properties, schema), which is a read of that object, so it is
   * authorized here against the read expression, and the create expression on the method must not
   * additionally deny a caller that only reads. When the object is absent, the request creates it,
   * so it falls through to the create expression on the method, which requires the create privilege
   * that the creation needs. A caller that can read but not create is denied exactly when its
   * request would create, and a caller that can create but not read is denied exactly when its
   * request would answer with metadata it cannot see, preserving normal creation of absent objects
   * as #12955 requires.
   *
   * <p>Nothing holds a lock across the resolution and the operation that follows it, so a
   * concurrent request can replace the addressed object between this check and the load the
   * operation performs. The interceptor therefore re-checks the read privilege after the operation,
   * before the result is returned; see {@link #invokeWithExistOkRecheck}.
   */
  private final class CreateModeAuthzHandler implements AuthorizationHandler {

    private static final String OVERWRITE_MODE = "OVERWRITE";
    private static final String EXIST_OK_MODE = "EXIST_OK";

    private final boolean overwrite;
    private final boolean existOk;
    private final String overwriteExpression;
    private final String readExpression;
    private boolean existOkReadAuthorized;

    private CreateModeAuthzHandler(String mode, String overwriteExpression, String readExpression) {
      // Read the mode through the same normalization the create operation applies, so a token the
      // operation will act on as an overwrite or exist_ok cannot be authorized as a plain create.
      String normalized = CommonUtil.normalizeToken(mode);
      this.overwrite = OVERWRITE_MODE.equals(normalized);
      this.existOk = EXIST_OK_MODE.equals(normalized);
      this.overwriteExpression = overwriteExpression;
      this.readExpression = readExpression;
    }

    @Override
    public void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
      Entity.EntityType entityType = deepestEntityType(nameIdentifierMap);
      if (overwrite) {
        boolean authorized = evaluateExpression(overwriteExpression, nameIdentifierMap, entityType);
        if (!authorized) {
          throw new ForbiddenException(
              "User '%s' is not authorized to overwrite '%s'",
              PrincipalUtils.getCurrentUserName(), nameIdentifierMap.get(entityType));
        }
      }
      if (existOk && objectExists(nameIdentifierMap)) {
        // The request answers with the existing object's metadata, so the read privilege decides,
        // and the create expression on the method must not run or a read-only caller could not
        // read.
        boolean authorized = evaluateExpression(readExpression, nameIdentifierMap, entityType);
        if (!authorized) {
          throw new ForbiddenException(
              "User '%s' is not authorized to read existing metadata for '%s' in exist_ok mode",
              PrincipalUtils.getCurrentUserName(), nameIdentifierMap.get(entityType));
        }
        existOkReadAuthorized = true;
      }
      // An exist_ok request for an absent object creates it, so the create expression on the
      // method must still run to authorize that creation.
    }

    @Override
    public boolean authorizationCompleted() {
      // An overwrite, and an exist_ok request that resolved to an existing object, are fully
      // authorized here. An exist_ok request for an absent object, and a plain create (neither
      // flag set), fall through to the create expression on the method unchanged.
      return overwrite || existOkReadAuthorized;
    }
  }

  /**
   * Evaluates an authorization expression against the identifiers resolved from a request.
   *
   * @param expression the expression to evaluate
   * @param nameIdentifierMap identifiers resolved from the request
   * @param entityType the entity type directly addressed by the request
   * @return {@code true} when the expression authorizes the request
   */
  private static boolean evaluateExpression(
      String expression,
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap,
      Entity.EntityType entityType) {
    return new AuthorizationExpressionEvaluator(expression)
        .evaluate(
            nameIdentifierMap,
            new HashMap<>(),
            new AuthorizationRequestContext(),
            Optional.of(entityType.name()));
  }

  /**
   * Returns whether the deepest object addressed by the given identifiers currently exists. The
   * check runs against the same metadata dispatchers the intercepted operations use, and it is only
   * reached in auxiliary mode, where metadata authorization is enabled and the dispatchers see the
   * caller's identity on the current thread.
   *
   * @param nameIdentifierMap identifiers resolved from the request
   * @return {@code true} when the addressed object exists
   */
  boolean objectExists(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
    switch (deepestEntityType(nameIdentifierMap)) {
      case TABLE:
        return GravitinoEnv.getInstance()
            .tableDispatcher()
            .tableExists(nameIdentifierMap.get(Entity.EntityType.TABLE));
      case SCHEMA:
        return GravitinoEnv.getInstance()
            .schemaDispatcher()
            .schemaExists(nameIdentifierMap.get(Entity.EntityType.SCHEMA));
      case CATALOG:
        return GravitinoEnv.getInstance()
            .catalogDispatcher()
            .catalogExists(nameIdentifierMap.get(Entity.EntityType.CATALOG));
      default:
        return false;
    }
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

  /**
   * Returns whether the invoked operation is a create request whose mode is {@code exist_ok},
   * reading the mode through the same normalization the create operation applies.
   *
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   * @return {@code true} when the request asks for {@code exist_ok} mode
   */
  private static boolean isExistOkCreate(Parameter[] parameters, Object[] args) {
    return createMode(parameters, args)
        .map(mode -> "EXIST_OK".equals(CommonUtil.normalizeToken(mode)))
        .orElse(false);
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
