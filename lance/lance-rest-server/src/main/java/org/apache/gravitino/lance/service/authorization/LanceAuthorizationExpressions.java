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
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.PROBE_SCHEMA_AUTHORIZATION_EXPRESSION;

/**
 * Authorization expressions for the Lance REST namespace surface.
 *
 * <p>A Lance identifier carries its depth rather than its kind: one level addresses a Gravitino
 * catalog, two a schema, and three a table. Every expression here therefore selects the privileges
 * to require with an {@code entityType} guard. The interceptor reports the addressed entity type,
 * and an identifier that resolves to any other type matches no branch and is therefore denied.
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

  /**
   * Authorizes listing the tables of a namespace. The identifier must address a schema, and the
   * tables the caller may not see are removed from the listing separately.
   */
  public static final String LIST_TABLES_AUTHORIZATION_EXPRESSION =
      "entityType == 'SCHEMA' && (" + LOAD_SCHEMA_AUTHORIZATION_EXPRESSION + ")";

  /**
   * Authorizes reading a table. The identifier must address a table, so an identifier that resolves
   * to a catalog or a schema is denied.
   */
  public static final String READ_TABLE_AUTHORIZATION_EXPRESSION =
      "entityType == 'TABLE' && (" + LOAD_TABLE_AUTHORIZATION_EXPRESSION + ")";

  /**
   * Authorizes a table existence probe. PROBE_TABLE_LIKE is the privilege that means exactly this,
   * and CREATE_TABLE is included as well because clients commonly check whether a table exists
   * immediately before creating it.
   *
   * <p>The endpoint answers with an empty 200 or a 404, so the only thing this expression grants is
   * knowledge of whether the table exists. Reading the table itself stays on {@link
   * #READ_TABLE_AUTHORIZATION_EXPRESSION}.
   *
   * <p>The Gravitino core server reaches the same conclusion by a different route: {@code
   * loadTable} returns the table body, so it keeps PROBE_TABLE_LIKE out of the primary expression
   * and admits it through {@code allowCheckExistence}, which only lets the request through when the
   * table is absent. That machinery is specific to the core interception service, and a dedicated
   * existence endpoint does not need it.
   */
  public static final String PROBE_TABLE_AUTHORIZATION_EXPRESSION =
      "entityType == 'TABLE' && (("
          + LOAD_TABLE_AUTHORIZATION_EXPRESSION
          + ") || (ANY_USE_CATALOG && ANY_USE_SCHEMA"
          + " && (ANY_PROBE_TABLE_LIKE || ANY_CREATE_TABLE)))";

  private LanceAuthorizationExpressions() {}
}
