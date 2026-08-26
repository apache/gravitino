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
import java.util.Optional;
import java.util.regex.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.common.ops.gravitino.ObjectIdentifier;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.lance.service.authorization.annotations.LanceRootNamespace;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.PermissionDeniedException;

/** Resolves Lance namespace IDs and maps shared authorization failures to Lance REST responses. */
public class LanceMetadataAuthorizationMethodInterceptor
    extends BaseMetadataAuthorizationMethodInterceptor implements MethodInterceptor {

  private static final int SCHEMA_NAMESPACE_LEVELS = 2;

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
    if (identifier.levels() == 0 || identifier.levels() > SCHEMA_NAMESPACE_LEVELS) {
      throw unsupportedIdentifier(targetId);
    }

    Map<Entity.EntityType, NameIdentifier> identifiers = baseIdentifiers();
    String catalogName = identifier.levelAtListPos(0);
    identifiers.put(
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalogName));
    if (identifier.levels() == SCHEMA_NAMESPACE_LEVELS) {
      identifiers.put(
          Entity.EntityType.SCHEMA,
          NameIdentifierUtil.ofSchema(metalakeName, catalogName, identifier.levelAtListPos(1)));
      return new AuthorizationTarget(identifiers, Entity.EntityType.SCHEMA);
    }
    return new AuthorizationTarget(identifiers, Entity.EntityType.CATALOG);
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

  private Map<Entity.EntityType, NameIdentifier> baseIdentifiers() {
    Map<Entity.EntityType, NameIdentifier> identifiers = new HashMap<>();
    identifiers.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    return identifiers;
  }

  private InvalidInputException unsupportedIdentifier(String namespaceId) {
    return new InvalidInputException(
        "Unsupported Lance namespace identifier: " + namespaceId, "", namespaceId);
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
