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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.server.authorization.GravitinoAuthorizer;

/** Evaluate the runtime result of the AuthorizationExpression.. */
public class AuthorizationExpressionEvaluator {

  /**
   * Use {@link AuthorizationConverter} to convert the authorization expression into an OGNL
   * expression, and then call {@link GravitinoAuthorizer} to perform permission verification.
   *
   * @param expression authorization expression
   */
  public AuthorizationExpressionEvaluator(String expression) {}

  /**
   * Use OGNL expressions to invoke GravitinoAuthorizer for authorizing multiple types of metadata
   * IDs.
   *
   * @param metadataIds key-metadata type, value-metadata id
   * @return authorization result
   */
  public boolean evaluate(Map<MetadataObject.Type, Long> metadataIds) {
    throw new UnsupportedOperationException();
  }
}
