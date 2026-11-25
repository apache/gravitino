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
package org.apache.gravitino.server.web.filter;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.dispatcher.RegisterTableSourceResolver;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/** Shared location authorization after destination privileges have been checked. */
final class IcebergTableLocationAuthzHelper {
  private static final Pattern SCHEMA_SPLIT =
      Pattern.compile(Pattern.quote(HierarchicalSchemaUtil.schemaSeparator()));

  private IcebergTableLocationAuthzHelper() {}

  static boolean allowed(
      Map<EntityType, NameIdentifier> identifiers, String expression, EntityType type) {
    return new AuthorizationExpressionEvaluator(expression)
        .evaluate(
            identifiers,
            new HashMap<>(),
            new AuthorizationRequestContext(),
            Optional.of(type.name()));
  }

  static void authorizeLocation(
      Map<EntityType, NameIdentifier> identifiers, String name, String location) {
    CatalogWrapperForREST wrapper =
        IcebergRESTServerContext.getInstance()
            .catalogWrapperManager()
            .getCatalogWrapper(identifiers.get(EntityType.CATALOG).name());
    TableIdentifier target =
        TableIdentifier.of(
            Namespace.of(SCHEMA_SPLIT.split(identifiers.get(EntityType.SCHEMA).name(), -1)), name);
    for (TableIdentifier source :
        RegisterTableSourceResolver.findOverlappingTables(wrapper, target, location)) {
      requireCanLoadTable(identifiers, source);
    }
  }

  static void requireCanLoadTable(
      Map<EntityType, NameIdentifier> destination, TableIdentifier source) {
    String metalake = destination.get(EntityType.METALAKE).name();
    String catalog = destination.get(EntityType.CATALOG).name();
    String schema =
        String.join(HierarchicalSchemaUtil.schemaSeparator(), source.namespace().levels());
    Map<EntityType, NameIdentifier> identifiers = new HashMap<>();
    identifiers.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
    identifiers.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalog));
    identifiers.put(EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, schema));
    identifiers.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalake, catalog, schema, source.name()));
    if (!allowed(identifiers, LOAD_TABLE_AUTHORIZATION_EXPRESSION, EntityType.TABLE)) {
      throw new ForbiddenException("Not authorized to use the requested table location");
    }
  }
}
