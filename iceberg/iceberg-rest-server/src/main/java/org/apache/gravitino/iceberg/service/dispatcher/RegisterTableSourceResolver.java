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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Resolves live catalog tables that share a register-table metadata file.
 *
 * <p>This class only discovers identities. Privilege checks belong in the Iceberg metadata
 * authorization interceptor handlers.
 */
public final class RegisterTableSourceResolver {

  private RegisterTableSourceResolver() {}

  /**
   * Finds tables other than {@code target} that share the given metadata file, UUID, or table
   * location.
   *
   * @param catalogWrapper catalog to scan
   * @param target register destination table
   * @param metadataLocation metadata file from the register request
   * @param metadata parsed metadata at that location
   * @return matching live table identities
   */
  public static List<TableIdentifier> findSourceTables(
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
    List<Namespace> children;
    List<TableIdentifier> tables;
    try {
      children = catalogWrapper.listNamespace(parent).namespaces();
      // Catalogs such as JDBC reject listing tables in the root namespace, and tables always live
      // in a named namespace, so only list tables below the root.
      tables = parent.isEmpty() ? List.of() : catalogWrapper.listTable(parent).identifiers();
    } catch (NoSuchNamespaceException e) {
      // The namespace was dropped while scanning.
      return;
    }
    for (Namespace child : children) {
      collectMatchingTables(
          catalogWrapper, child, target, uuid, tableLocation, metadataLocation, matches);
    }
    for (TableIdentifier identifier : tables) {
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
      // Only a table that no longer exists is unrelated. Any other failure to read its metadata
      // must not be mistaken for unrelated metadata, which would skip source authorization.
      return false;
    }
  }

  /** Normalizes path strings so {@code file:/} and {@code file:///} compare equal. */
  public static String normalizePath(String path) {
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
