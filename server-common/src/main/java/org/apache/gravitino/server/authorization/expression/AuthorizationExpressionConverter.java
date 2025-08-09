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

  /** Match ANY expressions */
  public static final Pattern ANY_PATTERN = Pattern.compile("ANY\\(([^)]+)\\)");

  /**
   * This authorization expression will invoke the `hasMetadataPrivilegePermission` method of
   * `GravitinoAuthorizer` to perform the access control check, and return the result of the
   * authorization.
   */
  public static final String CAN_OPERATE_METADATA_PRIVILEGE = "CAN_OPERATE_METADATA_PRIVILEGE";

  /**
   * This authorization expression will invoke the `hasSetOwnerPermission` method of
   * `GravitinoAuthorizer` to perform the access control check, and return the result of the
   * authorization.
   */
  public static final String CAN_SET_OWNER = "CAN_SET_OWNER";

  private static final String DENY_PREFIX = "DENY_";

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
          String replacedExpression = replaceAnyPrivilege(authorizationExpression);
          replacedExpression = replaceAnyExpressions(replacedExpression);
          Matcher matcher = PATTERN.matcher(replacedExpression);
          StringBuffer result = new StringBuffer();

          while (matcher.find()) {
            String type = matcher.group(1);
            String privilegeOrExpression = matcher.group(2);
            String replacement;
            if (AuthConstants.OWNER.equals(privilegeOrExpression)) {
              replacement = String.format("authorizer.isOwner(principal,METALAKE_NAME,%s)", type);
            } else if (privilegeOrExpression.startsWith(DENY_PREFIX)) {
              String privilege = privilegeOrExpression.substring(5);
              replacement =
                  String.format(
                      "authorizer.deny(principal,METALAKE_NAME,%s,"
                          + "@org.apache.gravitino.authorization.Privilege\\$Name@%s)",
                      type, privilege);
            } else if (AuthConstants.SELF.equals(privilegeOrExpression)) {
              replacement =
                  String.format(
                      "authorizer.isSelf(@org.apache.gravitino.Entity\\$EntityType@%s,%s_NAME_IDENT)",
                      type, type);
            } else {
              replacement =
                  String.format(
                      "authorizer.authorize(principal,METALAKE_NAME,%s,"
                          + "@org.apache.gravitino.authorization.Privilege\\$Name@%s)",
                      type, privilegeOrExpression);
            }

            matcher.appendReplacement(result, replacement);
          }
          matcher.appendTail(result);

          return result.toString();
        });
  }

  /**
   * Replaces any expression. For example, replace ANY(OWNER, METALAKE, CATALOG) to METALAKE::OWNER
   * || CATALOG::OWNER.
   *
   * @param expression The original expression
   * @return The modified expression
   */
  public static String replaceAnyExpressions(String expression) {
    Matcher matcher = ANY_PATTERN.matcher(expression);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String innerContent = matcher.group(1);
      String[] parts = innerContent.split(",");
      if (parts.length < 2) {
        matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(0)));
        continue;
      }

      String function = parts[0].trim();
      StringBuilder replacement = new StringBuilder();
      for (int i = 1; i < parts.length; i++) {
        String scope = parts[i].trim();
        if (!scope.isEmpty()) {
          if (replacement.length() > 0) {
            replacement.append(" || ");
          }
          replacement.append(scope).append("::").append(function);
        }
      }
      matcher.appendReplacement(result, replacement.toString());
    }
    matcher.appendTail(result);
    return result.toString();
  }

  /**
   * Replace any privilege expression to any expression
   *
   * @param expression authorization expression
   * @return authorization expression
   */
  public static String replaceAnyPrivilege(String expression) {
    expression = expression.replaceAll("SERVICE_ADMIN", "authorizer.isServiceAdmin()");
    expression = expression.replaceAll("METALAKE_USER", "authorizer.isMetalakeUser(METALAKE_NAME)");
    expression =
        expression.replaceAll(
            "ANY_USE_CATALOG",
            "((ANY(USE_CATALOG, METALAKE, CATALOG)) && "
                + "!(ANY(DENY_USE_CATALOG, METALAKE, CATALOG)))");
    expression =
        expression.replaceAll(
            "ANY_USE_SCHEMA",
            "((ANY(USE_SCHEMA, METALAKE, CATALOG, SCHEMA)) "
                + "&& !(ANY(DENY_USE_SCHEMA, METALAKE, CATALOG, SCHEMA)))");
    expression =
        expression.replaceAll(
            "ANY_CREATE_SCHEMA",
            "((ANY(CREATE_SCHEMA, METALAKE, CATALOG)) "
                + "&& !(ANY(DENY_CREATE_SCHEMA, METALAKE, CATALOG)))");
    expression =
        expression.replaceAll(
            "ANY_SELECT_TABLE",
            "((ANY(SELECT_TABLE, METALAKE, CATALOG, SCHEMA, TABLE)) "
                + "&& !(ANY(DENY_SELECT_TABLE, METALAKE, CATALOG, SCHEMA, TABLE)) )");
    expression =
        expression.replaceAll(
            "ANY_MODIFY_TABLE",
            "((ANY(MODIFY_TABLE, METALAKE, CATALOG, SCHEMA, TABLE)) "
                + "&& !(ANY(DENY_MODIFY_TABLE, METALAKE, CATALOG, SCHEMA, TABLE)))");
    expression =
        expression.replaceAll(
            "ANY_CREATE_TABLE",
            "((ANY(CREATE_TABLE, METALAKE, CATALOG, SCHEMA, TABLE)) "
                + "&& !(ANY(DENY_CREATE_TABLE, METALAKE, CATALOG, SCHEMA, TABLE)))");
    expression =
        expression.replaceAll(
            "ANY_CREATE_FILESET",
            "((ANY(CREATE_FILESET, METALAKE, CATALOG, SCHEMA, TABLE)) "
                + "&& !(ANY(DENY_CREATE_FILESET, METALAKE, CATALOG, SCHEMA, TABLE)))");
    expression =
        expression.replaceAll(
            "SCHEMA_OWNER_WITH_USE_CATALOG",
            "SCHEMA::OWNER && "
                + "((ANY(USE_CATALOG, METALAKE, CATALOG)) && "
                + "!(ANY(DENY_USE_CATALOG, METALAKE, CATALOG)))");
    expression =
        expression.replaceAll(
            "ANY_USE_MODEL",
            "((ANY(USE_MODEL, METALAKE, CATALOG, SCHEMA, MODEL)) && "
                + "!(ANY(DENY_USE_MODEL, METALAKE, CATALOG, SCHEMA, MODEL)))");
    expression =
        expression.replaceAll(
            "ANY_CREATE_MODEL_VERSION",
            "((ANY(CREATE_MODEL_VERSION, METALAKE, CATALOG, SCHEMA, MODEL)) "
                + "&& !(ANY(DENY_CREATE_MODEL_VERSION, METALAKE, CATALOG, SCHEMA, MODEL)))");
    expression =
        expression.replaceAll(
            "ANY_CREATE_MODEL",
            "((ANY(CREATE_MODEL, METALAKE, CATALOG, SCHEMA)) "
                + "&& !(ANY(DENY_CREATE_MODEL, METALAKE, CATALOG, SCHEMA)))");
    expression =
        expression.replaceAll(
            "ANY_CREATE_TOPIC",
            "((ANY(CREATE_TOPIC, METALAKE, CATALOG, SCHEMA, TOPIC)) "
                + "&& !(ANY(DENY_CREATE_TOPIC, METALAKE, CATALOG, SCHEMA, TOPIC)))");
    expression =
        expression.replaceAll(
            "ANY_PRODUCE_TOPIC",
            "((ANY(PRODUCE_TOPIC, METALAKE, CATALOG, SCHEMA, TOPIC))"
                + "&& !(ANY(DENY_PRODUCE_TOPIC, METALAKE, CATALOG, SCHEMA, TOPIC)))");
    expression =
        expression.replaceAll(
            "ANY_CONSUME_TOPIC",
            "((ANY(CONSUME_TOPIC, METALAKE, CATALOG, SCHEMA, TOPIC))"
                + "&& !(ANY(DENY_CONSUME_TOPIC, METALAKE, CATALOG, SCHEMA, TOPIC)))");
    expression =
        expression.replaceAll(
            "ANY_READ_FILESET",
            "((ANY(READ_FILESET, METALAKE, CATALOG, SCHEMA, FILESET))"
                + "&& !(ANY(DENY_READ_FILESET, METALAKE, CATALOG, SCHEMA, FILESET)))");
    expression =
        expression.replaceAll(
            "ANY_WRITE_FILESET",
            "((ANY(WRITE_FILESET, METALAKE, CATALOG, SCHEMA, FILESET))"
                + "&& !(ANY(DENY_WRITE_FILESET, METALAKE, CATALOG, SCHEMA, FILESET)))");
    expression =
        expression.replaceAll(
            CAN_SET_OWNER,
            "authorizer.hasSetOwnerPermission(p_metalake,p_metadataObjectType,p_fullName)");
    expression =
        expression.replaceAll(
            CAN_OPERATE_METADATA_PRIVILEGE,
            "authorizer.hasMetadataPrivilegePermission(p_metalake,p_type,p_fullName)");
    return expression;
  }
}
