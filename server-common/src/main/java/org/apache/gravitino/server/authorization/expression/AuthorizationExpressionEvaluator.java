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
import java.util.Map;
import ognl.Ognl;
import ognl.OgnlContext;
import ognl.OgnlException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
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
  public boolean evaluate(Map<Entity.EntityType, NameIdentifier> metadataNames) {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    GravitinoAuthorizer gravitinoAuthorizer =
        GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer();
    OgnlContext ognlContext = Ognl.createDefaultContext(null);
    ognlContext.put("principal", currentPrincipal);
    ognlContext.put("authorizer", gravitinoAuthorizer);
    metadataNames.forEach(
        (metadataType, metadataName) -> {
          MetadataObject metadataObject =
              NameIdentifierUtil.toMetadataObject(metadataName, metadataType);
          ognlContext.put(metadataType.name(), metadataObject);
        });
    NameIdentifier nameIdentifier = metadataNames.get(Entity.EntityType.METALAKE);
    ognlContext.put("METALAKE_NAME", nameIdentifier.name());
    try {
      Object value = Ognl.getValue(ognlAuthorizationExpression, ognlContext);
      return (boolean) value;
    } catch (OgnlException e) {
      throw new RuntimeException("ognl evaluate error", e);
    }
  }
}
