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

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

/** Evaluate the runtime result of the AuthorizationExpression. */
public class AuthorizationExpressionEvaluator {

  private final String ognlAuthorizationExpression;

  /**
   * Use {@link AuthorizationExpressionConverter} to convert the authorization expression into an
   * OGNL expression, and then call {@link GravitinoAuthorizer} to perform permission verification.
   *
   * @param expression authorization expression
   */
  public AuthorizationExpressionEvaluator(String expression) {
    this.ognlAuthorizationExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(expression);
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
    return evaluate(metadataNames, new HashMap<>(), requestContext);
  }

  /**
   * Use OGNL expressions to invoke GravitinoAuthorizer for authorizing multiple types of metadata
   * IDs.
   *
   * @param metadataNames key-metadata type, value-metadata NameIdentifier
   * @param pathParams params from request path
   * @return authorization result
   */
  public boolean evaluate(
      Map<Entity.EntityType, NameIdentifier> metadataNames,
      Map<String, Object> pathParams,
      AuthorizationRequestContext requestContext) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    GravitinoAuthorizer gravitinoAuthorizer =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer();
    OgnlContext ognlContext = Ognl.createDefaultContext(null);
    ognlContext.put("principal", currentPrincipal);
    ognlContext.put("authorizer", gravitinoAuthorizer);
    ognlContext.put("authorizationContext", requestContext);
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
      return (boolean) Ognl.getValue(ognlAuthorizationExpression, ognlContext);
    } catch (OgnlException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isMetadataType(Entity.EntityType type) {
    return Arrays.stream(MetadataObject.Type.values())
        .anyMatch(e -> Objects.equals(e.name(), type.name()));
  }
}
