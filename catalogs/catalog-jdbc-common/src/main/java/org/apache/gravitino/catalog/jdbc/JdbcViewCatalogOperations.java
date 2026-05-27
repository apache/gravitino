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
package org.apache.gravitino.catalog.jdbc;

import java.util.function.Predicate;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;

/**
 * Shared helper that implements {@link ViewCatalog} read methods for JDBC catalogs. Both MySQL and
 * PostgreSQL catalog operations classes delegate to this helper to avoid duplicating namespace
 * extraction and exception-handling logic.
 */
public class JdbcViewCatalogOperations implements ViewCatalog {

  private final JdbcViewOperations viewOps;
  private final Predicate<String> schemaExistsChecker;

  /**
   * Creates a new helper.
   *
   * @param viewOps The database-specific view operations.
   * @param schemaExistsChecker A predicate that checks whether a schema/database exists by name.
   */
  public JdbcViewCatalogOperations(
      JdbcViewOperations viewOps, Predicate<String> schemaExistsChecker) {
    this.viewOps = viewOps;
    this.schemaExistsChecker = schemaExistsChecker;
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    String databaseName = NameIdentifier.of(namespace.levels()).name();
    if (!schemaExistsChecker.test(databaseName)) {
      throw new NoSuchSchemaException("Schema %s does not exist", databaseName);
    }
    return viewOps.listViews(databaseName).stream()
        .map(name -> NameIdentifier.of(namespace, name))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    String databaseName = NameIdentifier.of(ident.namespace().levels()).name();
    return viewOps.load(databaseName, ident.name());
  }
}
