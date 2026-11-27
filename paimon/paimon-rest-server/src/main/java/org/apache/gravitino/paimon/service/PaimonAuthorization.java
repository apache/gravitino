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
package org.apache.gravitino.paimon.service;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.paimon.rest.exceptions.ForbiddenException;

/** Applies Gravitino's metadata authorization expressions to Paimon REST requests. */
public class PaimonAuthorization {
  private final boolean enabled;
  private final String metalake;

  /**
   * Creates the request authorization policy.
   *
   * @param enabled whether Gravitino authorization is enabled in auxiliary mode
   * @param metalake the registered catalogs' metalake
   */
  public PaimonAuthorization(boolean enabled, String metalake) {
    this.enabled = enabled;
    this.metalake = metalake;
  }

  String metalake() {
    return metalake;
  }

  /**
   * Evaluates access to catalog metadata.
   *
   * @param catalog registered catalog name
   * @param expression Gravitino authorization expression
   * @return whether the current principal has access
   */
  public boolean catalog(String catalog, String expression) {
    return !enabled
        || MetadataAuthzHelper.checkAccess(
            NameIdentifier.of(metalake, catalog), Entity.EntityType.CATALOG, expression);
  }

  /**
   * Evaluates access to schema metadata.
   *
   * @param catalog registered catalog name
   * @param database schema name
   * @param expression Gravitino authorization expression
   * @return whether the current principal has access
   */
  public boolean schema(String catalog, String database, String expression) {
    return !enabled
        || MetadataAuthzHelper.checkAccess(
            NameIdentifier.of(metalake, catalog, database), Entity.EntityType.SCHEMA, expression);
  }

  /**
   * Evaluates access to table metadata, including branch-qualified requests.
   *
   * @param catalog registered catalog name
   * @param database schema name
   * @param table base table name
   * @param expression Gravitino authorization expression
   * @return whether the current principal has access
   */
  public boolean table(String catalog, String database, String table, String expression) {
    return !enabled
        || MetadataAuthzHelper.checkAccess(
            NameIdentifier.of(metalake, catalog, database, table),
            Entity.EntityType.TABLE,
            expression);
  }

  /**
   * Rejects a request when the current principal lacks the required privileges.
   *
   * @param allowed whether authorization succeeded
   */
  public void require(boolean allowed) {
    if (!allowed) {
      throw new ForbiddenException("Access denied by Gravitino authorization.");
    }
  }
}
