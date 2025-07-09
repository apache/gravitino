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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConverter.ANY_PATTERN;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConverter.PATTERN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link AuthorizationExpressionConverter} */
public class TestAuthorizationExpressionConverter {

  /** Test for PATTERN. */
  @Test
  public void testPattern() {
    assertFalse(PATTERN.matcher("::").matches());
    assertFalse(PATTERN.matcher("KEY::").matches());
    assertFalse(PATTERN.matcher("::VALUE").matches());
    assertTrue(PATTERN.matcher("CATALOG::CREATE_TABLE").matches());
  }

  @Test
  public void testAnyPatter() {
    assertFalse(ANY_PATTERN.matcher("ANY").matches());
    assertFalse(ANY_PATTERN.matcher("ANYOWNER,METALAKE,CATALOG").matches());
    assertFalse(ANY_PATTERN.matcher("ANY(OWNER,METALAKE,CATALOG").matches());
    assertTrue(ANY_PATTERN.matcher("ANY(OWNER, METALAKE, CATALOG)").matches());
    assertTrue(ANY_PATTERN.matcher("ANY(USE_CATALOG,METALAKE,CATALOG,SCHEMA)").matches());
  }

  @Test
  public void testConvertToOgnlWithoutOwnerExpression() {
    String createTableAuthorizationExpression = "CATALOG::CREATE_TABLE || SCHEMA::CREATE_SCHEMA";
    String createTableOgnlExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(
            createTableAuthorizationExpression);
    Assertions.assertEquals(
        "authorizer.authorize(principal,METALAKE_NAME,CATALOG,"
            + "@org.apache.gravitino.authorization.Privilege$Name@CREATE_TABLE) "
            + "|| authorizer.authorize(principal,METALAKE_NAME,SCHEMA,"
            + "@org.apache.gravitino.authorization.Privilege$Name@CREATE_SCHEMA)",
        createTableOgnlExpression);
    String selectTableAuthorizationExpression =
        "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA &&"
            + " (TABLE::SELECT_TABLE || TABLE::MODIFY_TABLE)";
    String selectTableOgnlExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(
            selectTableAuthorizationExpression);
    Assertions.assertEquals(
        "authorizer.authorize(principal,METALAKE_NAME,CATALOG,"
            + "@org.apache.gravitino.authorization.Privilege$Name@USE_CATALOG) "
            + "&& authorizer.authorize(principal,METALAKE_NAME,SCHEMA,"
            + "@org.apache.gravitino.authorization.Privilege$Name@USE_SCHEMA) "
            + "&& (authorizer.authorize(principal,METALAKE_NAME,TABLE,"
            + "@org.apache.gravitino.authorization.Privilege$Name@SELECT_TABLE) "
            + "|| authorizer.authorize(principal,METALAKE_NAME,TABLE,"
            + "@org.apache.gravitino.authorization.Privilege$Name@MODIFY_TABLE))",
        selectTableOgnlExpression);
  }

  @Test
  public void testConvertToOgnlWithOwnerExpression() {
    String expressionWithOwner = "CATALOG::CREATE_SCHEMA || SCHEMA::OWNER";
    String createTableOgnlExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(expressionWithOwner);
    Assertions.assertEquals(
        "authorizer.authorize(principal,METALAKE_NAME,CATALOG,"
            + "@org.apache.gravitino.authorization.Privilege$Name@CREATE_SCHEMA) "
            + "|| authorizer.isOwner(principal,METALAKE_NAME,SCHEMA)",
        createTableOgnlExpression);

    String expressionWithOwner2 = "(ANY(OWNER, METALAKE, CATALOG)) && CATALOG::USE_CATALOG)";
    String useCatalogOgnExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(expressionWithOwner2);
    Assertions.assertEquals(
        "(authorizer.isOwner(principal,METALAKE_NAME,METALAKE) "
            + "|| authorizer.isOwner(principal,METALAKE_NAME,CATALOG))"
            + " && authorizer.authorize(principal,METALAKE_NAME,CATALOG"
            + ",@org.apache.gravitino.authorization.Privilege$Name@USE_CATALOG))",
        useCatalogOgnExpression);
  }

  @Test
  public void testReplaseAnyExpression() {
    Assertions.assertEquals(
        "METALAKE::USE_CATALOG || CATALOG::USE_CATALOG || CATALOG::OWNER",
        AuthorizationExpressionConverter.replaceAnyExpressions(
            "ANY(USE_CATALOG,METALAKE,CATALOG) || CATALOG::OWNER"));

    Assertions.assertEquals(
        "(METALAKE::USE_CATALOG || CATALOG::USE_CATALOG) || CATALOG::OWNER",
        AuthorizationExpressionConverter.replaceAnyExpressions(
            "(ANY(USE_CATALOG,METALAKE,CATALOG)) || CATALOG::OWNER"));

    Assertions.assertEquals(
        "METALAKE::OWNER || CATALOG::OWNER && CATALOG::OWNER",
        AuthorizationExpressionConverter.replaceAnyExpressions(
            "ANY(OWNER, METALAKE, CATALOG) && CATALOG::OWNER"));

    Assertions.assertEquals(
        "(METALAKE::OWNER || CATALOG::OWNER) && CATALOG::USE_CATALOG",
        AuthorizationExpressionConverter.replaceAnyExpressions(
            "(ANY(OWNER, METALAKE, CATALOG)) && CATALOG::USE_CATALOG"));
  }
}
