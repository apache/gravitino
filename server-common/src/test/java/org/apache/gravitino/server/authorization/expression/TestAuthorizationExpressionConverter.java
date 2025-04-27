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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link AuthorizationExpressionConverter} */
public class TestAuthorizationExpressionConverter {

  @Test
  public void testConvertToOgnlExpression() {
    String createTableAuthorizationExpression = "CATALOG::CREATE_TABLE || SCHEMA::CREATE_SCHEMA";
    String createTableOgnlExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(
            createTableAuthorizationExpression);
    Assertions.assertEquals(
        "authorizer.authorize(principal,METALAKE,CATALOG,'CREATE_TABLE') || authorizer.authorize(principal,METALAKE,SCHEMA,'CREATE_SCHEMA')",
        createTableOgnlExpression);
    String selectTableAuthorizationExpression =
        "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA && (TABLE::SELECT_TABLE || TABLE::MODIFY_TABLE)";
    String selectTableOgnlExpression =
        AuthorizationExpressionConverter.convertToOgnlExpression(
            selectTableAuthorizationExpression);
    Assertions.assertEquals(
        "authorizer.authorize(principal,METALAKE,CATALOG,'USE_CATALOG') && authorizer.authorize(principal,METALAKE,SCHEMA,'USE_SCHEMA') && (authorizer.authorize(principal,METALAKE,TABLE,'SELECT_TABLE') || authorizer.authorize(principal,METALAKE,TABLE,'MODIFY_TABLE'))",
        selectTableOgnlExpression);
  }
}
