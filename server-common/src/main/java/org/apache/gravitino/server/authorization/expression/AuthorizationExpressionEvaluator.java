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

package org.apache.gravitino.server.authorization.expression;

import com.google.common.annotations.VisibleForTesting;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import ognl.Ognl;
import ognl.OgnlContext;
import ognl.OgnlException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Evaluate the runtime result of the AuthorizationExpression. */
public class AuthorizationExpressionEvaluator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AuthorizationExpressionEvaluator.class);

  /**
   * Caches the OGNL AST parsed from an authorization expression string, so a single parsed tree can
   * be reused across evaluators and threads instead of re-parsing on every {@link #evaluate} call.
   *
   * <p>The shared tree is evaluated concurrently (see {@code MetadataAuthzHelper.doFilter}, which
   * evaluates on a thread pool). This is safe even though OGNL nodes lazily memoize constant
   * subexpressions during evaluation: only context-independent nodes are folded, every thread
   * writes the same immutable value, the write is published through a volatile flag, and each
   * evaluation builds its own {@link OgnlContext}, so no per-request state is shared.
   *
   * <p>The keys are the converted OGNL expression strings, which all originate from compile-time
   * constants in {@link AuthorizationExpressionConstants} and {@code @AuthorizationExpression}
   * annotations, so the cache is naturally bounded.
   */
  private static final Map<String, Object> PARSED_EXPRESSION_CACHE = new ConcurrentHashMap<>();

  private final Object ognlAuthorizationExpressionTree;
  private final GravitinoAuthorizer authorizer;

  /**
   * Use {@link AuthorizationExpressionConverter} to convert the authorization expression into an
   * OGNL expression, and then call {@link GravitinoAuthorizer} to perform permission verification.
   *
   * @param expression authorization expression
   */
  public AuthorizationExpressionEvaluator(String expression) {
    this(expression, GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer());
    LOGGER.debug("Authorization expression: {}", expression);
  }

  /**
   * Constructor of AuthorizationExpressionEvaluator
   *
   * @param expression authorization expression
   * @param authorizer GravitinoAuthorizer instance
   */
  public AuthorizationExpressionEvaluator(String expression, GravitinoAuthorizer authorizer) {
    this.ognlAuthorizationExpressionTree =
        parseExpression(AuthorizationExpressionConverter.convertToOgnlExpression(expression));
    this.authorizer = authorizer;
  }

  /**
   * Parses the OGNL expression into an AST, reusing the cached tree when the same expression was
   * parsed before.
   *
   * @param ognlExpression the converted OGNL expression string
   * @return the parsed OGNL AST, shared across evaluators for the same expression
   */
  private static Object parseExpression(String ognlExpression) {
    return PARSED_EXPRESSION_CACHE.computeIfAbsent(
        ognlExpression,
        expression -> {
          try {
            return Ognl.parseExpression(expression);
          } catch (OgnlException e) {
            throw new RuntimeException(
                "Failed to parse OGNL authorization expression: " + expression, e);
          }
        });
  }

  /**
   * Use OGNL expressions to invoke GravitinoAuthorizer for authorizing multiple types of metadata
   * IDs.
   *
   * @param metadataNames key-metadata type, value-metadata NameIdentifier
   * @return authorization result
   */
  public boolean evaluate(
      Map<Entity.EntityType, NameIdentifier> metadataNames,
      AuthorizationRequestContext requestContext) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    return evaluate(
        metadataNames, new HashMap<>(), requestContext, currentPrincipal, Optional.empty());
  }

  /**
   * Use OGNL expressions to invoke GravitinoAuthorizer for authorizing multiple types of metadata
   * IDs.
   *
   * @param metadataNames key-metadata type, value-metadata NameIdentifier
   * @param requestContext authorization request context
   * @param principal current principal
   * @param entityType entityType
   * @return authorization result
   */
  public boolean evaluate(
      Map<Entity.EntityType, NameIdentifier> metadataNames,
      AuthorizationRequestContext requestContext,
      Principal principal,
      Optional<String> entityType) {
    return evaluate(metadataNames, new HashMap<>(), requestContext, principal, entityType);
  }

  /**
   * Use OGNL expressions to invoke GravitinoAuthorizer for authorizing multiple types of metadata
   * IDs.
   *
   * @param metadataNames key-metadata type, value-metadata NameIdentifier
   * @param pathParams params from request path
   * @param entityType entityType
   * @return authorization result
   */
  public boolean evaluate(
      Map<Entity.EntityType, NameIdentifier> metadataNames,
      Map<String, Object> pathParams,
      AuthorizationRequestContext requestContext,
      Optional<String> entityType) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    return evaluate(metadataNames, pathParams, requestContext, currentPrincipal, entityType);
  }

  /**
   * Use OGNL expressions to invoke GravitinoAuthorizer for authorizing multiple types of metadata
   * IDs.
   *
   * @param metadataNames key-metadata type, value-metadata NameIdentifier
   * @param pathParams params from request path
   * @param requestContext authorization request context
   * @param currentPrincipal current principal
   * @return authorization result
   */
  private boolean evaluate(
      Map<Entity.EntityType, NameIdentifier> metadataNames,
      Map<String, Object> pathParams,
      AuthorizationRequestContext requestContext,
      Principal currentPrincipal,
      Optional<String> entityType) {
    OgnlContext ognlContext = Ognl.createDefaultContext(null);
    ognlContext.put("principal", currentPrincipal);
    ognlContext.put("authorizer", authorizer);
    ognlContext.put("authorizationContext", requestContext);
    ognlContext.put("entityType", entityType.orElse(null));
    ognlContext.putAll(pathParams);
    metadataNames.forEach(
        (type, entityNameIdent) -> {
          if (isMetadataType(type)) {
            MetadataObject metadataObject =
                NameIdentifierUtil.toMetadataObject(entityNameIdent, type);
            ognlContext.put(type.name(), metadataObject);
          }
          ognlContext.put(type.name() + "_NAME_IDENT", entityNameIdent);
        });
    NameIdentifier nameIdentifier = metadataNames.get(Entity.EntityType.METALAKE);
    ognlContext.put(
        "METALAKE_NAME", Optional.ofNullable(nameIdentifier).map(NameIdentifier::name).orElse(""));
    try {
      return (boolean) Ognl.getValue(ognlAuthorizationExpressionTree, ognlContext);
    } catch (OgnlException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isMetadataType(Entity.EntityType type) {
    return Arrays.stream(MetadataObject.Type.values())
        .anyMatch(e -> Objects.equals(e.name(), type.name()));
  }

  /**
   * Returns the parsed OGNL AST backing this evaluator, for tests asserting that the same
   * expression reuses one cached tree.
   *
   * @return the parsed OGNL AST
   */
  @VisibleForTesting
  Object getOgnlAuthorizationExpressionTree() {
    return ognlAuthorizationExpressionTree;
  }

  /** Clears the parsed-expression cache so tests can run in isolation. */
  @VisibleForTesting
  static void clearParsedExpressionCache() {
    PARSED_EXPRESSION_CACHE.clear();
  }
}
