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
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blocks Iceberg REST {@code registerTable} from adopting metadata that still belongs to another
 * table the caller cannot read.
 *
 * <p>Schema-level {@code CREATE_TABLE} alone is not enough: registering a readable copy of an
 * unreadable table's metadata file would otherwise mint a new owned table that exposes the same
 * schema and data locations. When authorization is enabled, this check finds catalog tables that
 * share the metadata file, table UUID, or table location, and requires the load-table expression on
 * each such source (other than the register target itself). Orphaned metadata with no live match
 * remains allowed so external / post-drop registration keeps working.
 */
@SuppressWarnings("FormatStringAnnotation")
final class RegisterTableSourceAuthorization {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterTableSourceAuthorization.class);

  private RegisterTableSourceAuthorization() {}

  /**
   * Authorizes adoption of {@code metadata} for the register target when authorization is enabled.
   *
   * @param catalogName Iceberg catalog name
   * @param targetNamespace register destination namespace
   * @param targetTableName register destination table name
   * @param metadataLocation metadata file path from the request
   * @param metadata parsed table metadata
   * @param catalogWrapper catalog used to discover live tables that share the metadata
   */
  static void authorize(
      String catalogName,
      Namespace targetNamespace,
      String targetTableName,
      String metadataLocation,
      TableMetadata metadata,
      CatalogWrapperForREST catalogWrapper) {
    IcebergRESTServerContext context;
    try {
      context = IcebergRESTServerContext.getInstance();
    } catch (IllegalStateException e) {
      // Unit tests and non-REST entry points may call the executor without a server context.
      return;
    }
    if (!context.isAuthorizationEnabled()) {
      return;
    }
    if (context.skipAuthorizationForRestBackend() && catalogWrapper.isRESTCatalog()) {
      return;
    }

    TableIdentifier target = TableIdentifier.of(targetNamespace, targetTableName);
    List<TableIdentifier> sources =
        findSourceTables(catalogWrapper, target, metadataLocation, metadata);
    if (sources.isEmpty()) {
      return;
    }

    String metalake = context.metalakeName();
    for (TableIdentifier source : sources) {
      requireCanLoadTable(metalake, catalogName, source);
    }
  }

  static List<TableIdentifier> findSourceTables(
      IcebergCatalogWrapper catalogWrapper,
      TableIdentifier target,
      String metadataLocation,
      TableMetadata metadata) {
    List<TableIdentifier> matches = new ArrayList<>();
    String uuid = metadata.uuid();
    String tableLocation = normalizePath(metadata.location());
    String normalizedMeta = normalizePath(metadataLocation);
    collectMatchingTables(
        catalogWrapper, Namespace.empty(), target, uuid, tableLocation, normalizedMeta, matches);
    return matches;
  }

  private static void collectMatchingTables(
      IcebergCatalogWrapper catalogWrapper,
      Namespace parent,
      TableIdentifier target,
      String uuid,
      String tableLocation,
      String metadataLocation,
      List<TableIdentifier> matches) {
    for (Namespace child : catalogWrapper.listNamespace(parent).namespaces()) {
      collectMatchingTables(
          catalogWrapper, child, target, uuid, tableLocation, metadataLocation, matches);
    }
    for (TableIdentifier identifier : catalogWrapper.listTable(parent).identifiers()) {
      if (identifier.equals(target)) {
        continue;
      }
      if (matchesSource(catalogWrapper, identifier, uuid, tableLocation, metadataLocation)) {
        matches.add(identifier);
      }
    }
  }

  private static boolean matchesSource(
      IcebergCatalogWrapper catalogWrapper,
      TableIdentifier identifier,
      String uuid,
      String tableLocation,
      String metadataLocation) {
    Optional<String> currentMeta = catalogWrapper.getTableMetadataLocation(identifier);
    if (currentMeta.isPresent() && normalizePath(currentMeta.get()).equals(metadataLocation)) {
      return true;
    }
    try {
      TableMetadata existing = catalogWrapper.loadTableMetadata(identifier);
      if (uuid != null && uuid.equals(existing.uuid())) {
        return true;
      }
      return tableLocation != null
          && !tableLocation.isEmpty()
          && tableLocation.equals(normalizePath(existing.location()));
    } catch (NoSuchTableException e) {
      return false;
    } catch (RuntimeException e) {
      LOG.debug(
          "Skip table {} while resolving register-table source: {}", identifier, e.toString());
      return false;
    }
  }

  private static void requireCanLoadTable(
      String metalake, String catalogName, TableIdentifier source) {
    String schema =
        String.join(HierarchicalSchemaUtil.schemaSeparator(), source.namespace().levels());
    Map<EntityType, NameIdentifier> identifiers = new HashMap<>();
    identifiers.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
    identifiers.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalogName));
    identifiers.put(EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalogName, schema));
    identifiers.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalake, catalogName, schema, source.name()));

    boolean authorized =
        new AuthorizationExpressionEvaluator(LOAD_TABLE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                identifiers,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.of(EntityType.TABLE.name()));
    if (!authorized) {
      throw new ForbiddenException(
          "User '%s' is not authorized to register metadata from table '%s'",
          PrincipalUtils.getCurrentUserName(), source);
    }
  }

  /** Normalizes path strings so {@code file:/} and {@code file:///} compare equal. */
  static String normalizePath(String path) {
    if (path == null) {
      return "";
    }
    String trimmed = path.trim();
    if (trimmed.startsWith("file:")) {
      String withoutScheme = trimmed.substring("file:".length());
      while (withoutScheme.startsWith("//")) {
        withoutScheme = withoutScheme.substring(1);
      }
      if (!withoutScheme.startsWith("/")) {
        withoutScheme = "/" + withoutScheme;
      }
      return "file:" + withoutScheme;
    }
    if (trimmed.endsWith("/")) {
      return trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }
}
