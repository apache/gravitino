/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CAN_ACCESS_METADATA;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.PROBE_SCHEMA_AUTHORIZATION_EXPRESSION;

/**
 * Authorization expressions for the Lance REST namespace surface.
 *
 * <p>A Lance namespace identifier addresses a Gravitino catalog at one level and a Gravitino schema
 * at two levels, so every expression here selects the privileges to require with an {@code
 * entityType} guard. The interceptor reports the addressed entity type, and an identifier that
 * resolves to any other type matches no branch and is therefore denied.
 */
public final class LanceAuthorizationExpressions {

  /**
   * Authorizes a namespace existence probe. Catalog probes use normal catalog access. Schema probes
   * additionally permit CREATE_SCHEMA, because clients commonly check existence immediately before
   * creating a schema.
   */
  public static final String PROBE_NAMESPACE_AUTHORIZATION_EXPRESSION =
      CAN_ACCESS_METADATA
          + " || (entityType == 'SCHEMA' && ("
          + PROBE_SCHEMA_AUTHORIZATION_EXPRESSION
          + "))";

  /**
   * Authorizes creating a namespace. A one-level namespace creates a catalog and therefore requires
   * CREATE_CATALOG on the metalake; a two-level namespace creates a schema and requires
   * CREATE_SCHEMA together with access to the parent catalog.
   *
   * <p>This expression covers the {@code create} and {@code exist_ok} modes only. The {@code
   * overwrite} mode alters an existing namespace, so it is authorized against {@link
   * #MODIFY_NAMESPACE_AUTHORIZATION_EXPRESSION} instead and a create privilege alone never grants
   * it.
   */
  public static final String CREATE_NAMESPACE_AUTHORIZATION_EXPRESSION =
      """
      (entityType == 'CATALOG' && (METALAKE::OWNER || METALAKE::CREATE_CATALOG)) ||
      (entityType == 'SCHEMA' && (ANY(OWNER, METALAKE, CATALOG) ||
      ANY_USE_CATALOG && ANY_CREATE_SCHEMA))
      """;

  /**
   * Authorizes altering or dropping an existing namespace. Both require ownership of the namespace
   * or of one of its ancestors, matching the Gravitino and Iceberg REST surfaces.
   */
  public static final String MODIFY_NAMESPACE_AUTHORIZATION_EXPRESSION =
      """
      (entityType == 'CATALOG' && ANY(OWNER, METALAKE, CATALOG)) ||
      (entityType == 'SCHEMA' && (ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG))
      """;

  private LanceAuthorizationExpressions() {}
}
