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

package org.apache.gravitino.server.web.rest.authorization;

import java.util.Set;
import java.util.regex.Matcher;
import ognl.Ognl;
import ognl.OgnlContext;
import ognl.OgnlException;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConverter;

/** MockAuthorizationExpressionEvaluator for test authorization in rest api. */
public class MockAuthorizationExpressionEvaluator {

  private final String ognlExpression;

  public MockAuthorizationExpressionEvaluator(String expression) {
    expression = AuthorizationExpressionConverter.replaceAnyPrivilege(expression);
    expression = AuthorizationExpressionConverter.replaceAnyExpressions(expression);
    Matcher matcher = AuthorizationExpressionConverter.PATTERN.matcher(expression);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      String metadataPrivilege = matcher.group(0);
      String replacement;
      replacement = String.format("authorizer.authorize('%s')", metadataPrivilege);
      matcher.appendReplacement(result, replacement);
    }
    matcher.appendTail(result);
    this.ognlExpression = result.toString();
  }

  /**
   * mock authorization with privilege
   *
   * @param mockPrivileges mock user has some privilege
   * @return mock authorization result
   * @throws OgnlException OgnlException
   */
  public boolean getResult(Set<String> mockPrivileges) throws OgnlException {
    MockAuthorizer mockAuthorizer = new MockAuthorizer(mockPrivileges);
    OgnlContext ognlContext = Ognl.createDefaultContext(null);
    ognlContext.put("authorizer", mockAuthorizer);
    Object value = Ognl.getValue(ognlExpression, ognlContext);
    return (boolean) value;
  }

  private static final class MockAuthorizer {

    private Set<String> mockPrivilege;

    private MockAuthorizer(Set<String> mockPrivilege) {
      this.mockPrivilege = mockPrivilege;
    }

    public boolean authorize(String metadataPrivilege) {
      return mockPrivilege.contains(metadataPrivilege);
    }
  }
}
