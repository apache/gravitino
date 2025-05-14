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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.server.authorization.MetadataFilterHelper;

/**
 * Convert the authorization expression into an executable expression, such as OGNL expression, etc.
 */
public class AuthorizationExpressionConverter {

  /** Match authorization expressions */
  public static final Pattern PATTERN = Pattern.compile("([A-Z_]+)::([A-Z_]+)");

  /**
   * The EXPRESSION_CACHE caches the result of converting authorization expressions into an OGNL
   * expression.
   */
  private static final Map<String, String> EXPRESSION_CACHE = new ConcurrentHashMap<>();

  private AuthorizationExpressionConverter() {}

  /**
   * Convert the authorization expression to OGNL expression. <a
   * href="https://github.com/orphan-oss/ognl">OGNL</a> stands for Object-Graph Navigation Language;
   * It is an expression language for getting and setting properties of Java objects, plus other
   * extras such as list projection and selection and lambda expressions. You use the same
   * expression for both getting and setting the value of a property.
   *
   * @param authorizationExpression authorization expression from {@link MetadataFilterHelper}
   * @return an OGNL expression used to call GravitinoAuthorizer
   */
  public static String convertToOgnlExpression(String authorizationExpression) {
    return EXPRESSION_CACHE.computeIfAbsent(
        authorizationExpression,
        (expression) -> {
          Matcher matcher = PATTERN.matcher(expression);
          StringBuffer result = new StringBuffer();

          while (matcher.find()) {
            String metadataType = matcher.group(1);
            String privilegeOrOwner = matcher.group(2);
            String replacement;
            if (AuthConstants.OWNER.equals(privilegeOrOwner)) {
              replacement =
                  String.format("authorizer.isOwner(principal,METALAKE_NAME,%s)", metadataType);
            } else {
              replacement =
                  String.format(
                      "authorizer.authorize(principal,METALAKE_NAME,%s,"
                          + "@org.apache.gravitino.authorization.Privilege\\$Name@%s)",
                      metadataType, privilegeOrOwner);
            }
            matcher.appendReplacement(result, replacement);
          }
          matcher.appendTail(result);

          return result.toString();
        });
  }
}
