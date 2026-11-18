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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.catalog.EntityCombinedTable;
import org.apache.gravitino.catalog.EntityCombinedView;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.semantic.SemanticModelSourceUpdate;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Validates source visibility and existence before model writes. */
public class SemanticModelSourceValidator {
  private final TableDispatcher tables;
  private final ViewDispatcher views;
  private final Supplier<GravitinoAuthorizer> authorizer;
  private final Function<NameIdentifier, Set<Entity.EntityType>> supportedSourceTypes;

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
    this(
        tables,
        views,
        authorizer,
        ident ->
            GravitinoEnv.getInstance()
                .catalogManager()
                .doWithCatalog(
                    NameIdentifierUtil.getCatalogIdentifier(ident),
                    catalog -> {
                      CatalogOperations ops = catalog.ops();
                      Set<Entity.EntityType> types = EnumSet.noneOf(Entity.EntityType.class);
                      if (ops instanceof TableCatalog) {
                        types.add(Entity.EntityType.TABLE);
                      }
                      if (ops instanceof ViewCatalog) {
                        types.add(Entity.EntityType.VIEW);
                      }
                      return types;
                    }));
  }

  SemanticModelSourceValidator(
      TableDispatcher tables,
      ViewDispatcher views,
      Supplier<GravitinoAuthorizer> authorizer,
      Function<NameIdentifier, Set<Entity.EntityType>> supportedSourceTypes) {
    this.supportedSourceTypes = supportedSourceTypes;
    this.tables = tables;
    this.views = views;
    this.authorizer = authorizer;
  }

  /**
   * Validates every dataset source under the current caller.
   *
   * <p>Key and relationship references resolve to fields declared inside the model and are checked
   * by {@code SemanticModelValidator}; this validator only resolves the datasets' sources against
   * the catalog. This method must run even when authorization is disabled. It does not interpret
   * SQL expressions or validate transitive view dependencies. Connection failures propagate
   * unchanged.
   *
   * @param metalake the request's metalake, also used for cross-catalog source references
   * @param definition the complete definition to validate before persistence
   * @return The complete source associations resolved by this validation.
   */
  public SemanticModelSourceUpdate validate(String metalake, SemanticModelDefinition definition) {
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    GravitinoAuthorizer currentAuthorizer = authorizer.get();
    Map<NameIdentifier, ResolvedSource> resolvedSources = new HashMap<>();
    List<SemanticModelSource> associations = new ArrayList<>();
    for (Dataset dataset : definition.datasets()) {
      NameIdentifier source = dataset.source();
      if (source.namespace().length() != 2) {
        throw new IllegalSemanticModelException("Source must contain catalog.schema.name");
      }
      NameIdentifier ident =
          NameIdentifier.of(
              metalake, source.namespace().level(0), source.namespace().level(1), source.name());
      ResolvedSource resolved =
          resolvedSources.computeIfAbsent(
              ident, key -> loadSource(key, currentAuthorizer, context));
      associations.add(new SemanticModelSource(dataset.name(), resolved.type, resolved.id));
    }
    return SemanticModelSourceUpdate.replace(associations);
  }

  private ResolvedSource loadSource(
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
    boolean viewAllowed =
        canLoad(
            ident,
            Entity.EntityType.VIEW,
            AuthorizationExpressionConstants.LOAD_VIEW_AUTHORIZATION_EXPRESSION,
            currentAuthorizer,
            context);
    if (!tableAllowed && !viewAllowed) {
      throw new ForbiddenException("Not authorized to resolve Semantic Model source %s", ident);
    }
    Set<Entity.EntityType> supported = EnumSet.of(Entity.EntityType.TABLE, Entity.EntityType.VIEW);
    try {
      supported = supportedSourceTypes.apply(ident);
      if (tableAllowed && supported.contains(Entity.EntityType.TABLE)) {
        try {
          Table table = tables.loadTable(ident);
          return new ResolvedSource(registeredTableId(ident, table), Entity.EntityType.TABLE);
        } catch (NotFoundException missing) {
          // An untyped source can also refer to a view.
        }
      }
      if (viewAllowed && supported.contains(Entity.EntityType.VIEW)) {
        View view = views.loadView(ident);
        return new ResolvedSource(registeredViewId(ident, view), Entity.EntityType.VIEW);
      }
    } catch (NotFoundException missing) {
      // Do not disclose whether the other, inaccessible object type exists.
    }
    if ((supported.contains(Entity.EntityType.TABLE) && !tableAllowed)
        || (supported.contains(Entity.EntityType.VIEW) && !viewAllowed)) {
      throw new ForbiddenException("Not authorized to resolve Semantic Model source %s", ident);
    }
    throw new IllegalSemanticModelException("Semantic Model source %s does not exist", ident);
  }

  private static long registeredTableId(NameIdentifier ident, Table table) {
    if (table instanceof EntityCombinedTable) {
      EntityCombinedTable combined = (EntityCombinedTable) table;
      if (combined.tableFromGravitino() != null) {
        return combined.tableFromGravitino().id();
      }
      // Managed tables carry their stored ID in the loaded catalog properties, without a separate
      // TableEntity. Read it before public-property masking, without resolving the name again.
      if (combined.imported()) {
        StringIdentifier id =
            StringIdentifier.fromProperties(combined.tableFromCatalog().properties());
        if (id != null) {
          return id.id();
        }
      }
    }
    throw new IllegalStateException("Source table has no registered Gravitino identity: " + ident);
  }

  private static long registeredViewId(NameIdentifier ident, View view) {
    if (view instanceof EntityCombinedView) {
      EntityCombinedView combined = (EntityCombinedView) view;
      if (combined.viewEntity() != null) {
        return combined.viewEntity().id();
      }
      // Managed views use the same stored-identity property as managed tables.
      if (combined.imported()) {
        StringIdentifier id =
            StringIdentifier.fromProperties(combined.viewFromCatalog().properties());
        if (id != null) {
          return id.id();
        }
      }
    }
    throw new IllegalStateException("Source view has no registered Gravitino identity: " + ident);
  }

  private static class ResolvedSource {
    private final long id;
    private final Entity.EntityType type;

    private ResolvedSource(long id, Entity.EntityType type) {
      this.id = id;
      this.type = type;
    }
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
}
