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
package org.apache.gravitino.server.web.rest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Validates source visibility, existence, and explicit column references before model writes. */
public class SemanticModelSourceValidator {
  private final TableDispatcher tables;
  private final ViewDispatcher views;
  private final Supplier<GravitinoAuthorizer> authorizer;

  /**
   * Creates a validator using the server's authorizer, including its disabled-mode pass-through.
   *
   * @param tables table metadata dispatcher
   * @param views view metadata dispatcher
   */
  @Inject
  public SemanticModelSourceValidator(TableDispatcher tables, ViewDispatcher views) {
    this(tables, views, () -> GravitinoAuthorizerProvider.getInstance().getGravitinoAuthorizer());
  }

  SemanticModelSourceValidator(
      TableDispatcher tables, ViewDispatcher views, Supplier<GravitinoAuthorizer> authorizer) {
    this.tables = tables;
    this.views = views;
    this.authorizer = authorizer;
  }

  /**
   * Validates every dataset source and key or relationship column under the current caller.
   *
   * <p>This method must run even when authorization is disabled. It does not interpret SQL
   * expressions or validate transitive view dependencies. Connection failures propagate unchanged.
   *
   * @param metalake the request's metalake, also used for cross-catalog source references
   * @param definition the complete definition to validate before persistence
   */
  public void validate(String metalake, SemanticModelDefinition definition) {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    GravitinoAuthorizer currentAuthorizer = authorizer.get();
    Map<String, Set<String>> datasetColumns = new HashMap<>();
    Map<NameIdentifier, Set<String>> sourceColumns = new HashMap<>();
    for (Dataset dataset : definition.datasets()) {
      NameIdentifier source = dataset.source();
      if (source.namespace().length() != 2) {
        throw new IllegalSemanticModelException("Source must contain catalog.schema.name");
      }
      NameIdentifier ident =
          NameIdentifier.of(
              metalake, source.namespace().level(0), source.namespace().level(1), source.name());
      Set<String> columns =
          sourceColumns.computeIfAbsent(ident, key -> loadColumns(key, currentAuthorizer, context));
      datasetColumns.put(dataset.name(), columns);
      checkColumns(dataset.name(), dataset.primaryKey(), columns);
      if (dataset.uniqueKeys() != null) {
        for (String[] key : dataset.uniqueKeys()) {
          checkColumns(dataset.name(), key, columns);
        }
      }
    }
    if (definition.relationships() != null) {
      for (Relationship relationship : definition.relationships()) {
        checkColumns(
            relationship.from(),
            relationship.fromColumns(),
            datasetColumns.get(relationship.from()));
        checkColumns(
            relationship.to(), relationship.toColumns(), datasetColumns.get(relationship.to()));
      }
    }
  }

  private Set<String> loadColumns(
      NameIdentifier ident,
      GravitinoAuthorizer currentAuthorizer,
      AuthorizationRequestContext context) {
    boolean tableAllowed =
        canLoad(
            ident,
            Entity.EntityType.TABLE,
            AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION,
            currentAuthorizer,
            context);
    if (tableAllowed) {
      try {
        return columnNames(ident, tables.loadTable(ident).columns());
      } catch (NotFoundException missing) {
        // The untyped source can also refer to a logical view. Authorize that lookup separately.
      }
    }
    boolean viewAllowed =
        canLoad(
            ident,
            Entity.EntityType.VIEW,
            AuthorizationExpressionConstants.LOAD_VIEW_AUTHORIZATION_EXPRESSION,
            currentAuthorizer,
            context);
    if (viewAllowed) {
      try {
        return columnNames(ident, views.loadView(ident).columns());
      } catch (NotFoundException missing) {
        // Do not disclose whether the other, inaccessible object type exists.
      }
    }
    if (!tableAllowed || !viewAllowed) {
      throw new ForbiddenException("Not authorized to resolve Semantic Model source %s", ident);
    }
    throw new IllegalSemanticModelException("Semantic Model source %s does not exist", ident);
  }

  private boolean canLoad(
      NameIdentifier ident,
      Entity.EntityType type,
      String expression,
      GravitinoAuthorizer currentAuthorizer,
      AuthorizationRequestContext context) {
    return new AuthorizationExpressionEvaluator(expression, currentAuthorizer)
        .evaluate(
            NameIdentifierUtil.splitNameIdentifier(ident.namespace().level(0), type, ident),
            context);
  }

  private Set<String> columnNames(NameIdentifier ident, Column[] columns) {
    if (columns == null) {
      throw new IllegalSemanticModelException(
          "Column metadata is unavailable for source %s", ident);
    }
    Set<String> names = new HashSet<>();
    Arrays.stream(columns).forEach(column -> names.add(column.name()));
    return names;
  }

  private void checkColumns(String dataset, String[] references, Set<String> columns) {
    if (columns == null) {
      throw new IllegalSemanticModelException("Unknown dataset %s in relationship", dataset);
    }
    if (references != null) {
      for (String reference : references) {
        if (!columns.contains(reference)) {
          throw new IllegalSemanticModelException(
              "Dataset %s source has no column %s", dataset, reference);
        }
      }
    }
  }
}
