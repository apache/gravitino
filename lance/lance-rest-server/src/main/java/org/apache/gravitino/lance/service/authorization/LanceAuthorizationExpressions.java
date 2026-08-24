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

import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;

/** Authorization expressions used by the Lance REST metadata operations. */
public class LanceAuthorizationExpressions {

  /** Read a catalog-level Lance namespace: describe, exists and list its schemas. */
  public static final String READ_CATALOG_NAMESPACE =
      AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION;

  /** Read a schema-level Lance namespace: describe and list. */
  public static final String READ_SCHEMA_NAMESPACE =
      AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION;

  /**
   * Check the existence of a schema-level Lance namespace.
   *
   * <p>CREATE_SCHEMA is accepted here, like in the Iceberg REST service, because clients probe a
   * namespace for existence before creating it. Describe stays on the stricter expression so that a
   * principal who may only create a schema cannot read the properties of an existing one.
   */
  public static final String SCHEMA_NAMESPACE_EXISTS =
      "ANY(OWNER, METALAKE, CATALOG) || ANY_USE_CATALOG && (SCHEMA::OWNER || ANY_USE_SCHEMA || ANY_CREATE_SCHEMA)";

  private LanceAuthorizationExpressions() {}
}
