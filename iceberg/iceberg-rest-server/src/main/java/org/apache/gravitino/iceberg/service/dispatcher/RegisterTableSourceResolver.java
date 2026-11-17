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
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.utils.IcebergTableLocationUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves live catalog tables that share a register-table metadata file.
 *
 * <p>This class only discovers identities. Privilege checks belong in the Iceberg metadata
 * authorization interceptor handlers.
 */
public final class RegisterTableSourceResolver {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterTableSourceResolver.class);

  private RegisterTableSourceResolver() {}

  /**
   * Finds tables other than {@code target} whose storage locations overlap the registered table.
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
    return findOverlappingTables(catalogWrapper, target, metadata.location());
  }

  /**
   * Finds overlapping tables using a bulk pointer lookup when the backend supports it. Pointer
   * inference requires the standard {@code <table>/metadata/<file>} layout. Non-JDBC backends
   * retain namespace discovery, with unreadable table metadata logged and skipped.
   *
   * @param catalogWrapper catalog to query
   * @param target destination to exclude
   * @param location requested table location
   * @return overlapping live table identities
   */
  public static List<TableIdentifier> findOverlappingTables(
      IcebergCatalogWrapper catalogWrapper, TableIdentifier target, String location) {
    List<TableIdentifier> matches = new ArrayList<>();
    if (normalizePath(location).isEmpty()) {
      return matches;
    }
    Optional<Map<TableIdentifier, String>> candidates =
        catalogWrapper.getTableMetadataLocationCandidates(location);
    if (candidates.isPresent()) {
      candidates
          .get()
          .forEach(
              (identifier, pointer) -> {
                if (!identifier.equals(target) && matchesPointer(pointer, location)) {
                  matches.add(identifier);
                }
              });
      return matches;
    }
    collectMatchingTables(catalogWrapper, Namespace.empty(), target, location, matches);
    return matches;
  }

  private static void collectMatchingTables(
      IcebergCatalogWrapper catalogWrapper,
      Namespace parent,
      TableIdentifier target,
      String tableLocation,
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
      collectMatchingTables(catalogWrapper, child, target, tableLocation, matches);
    }
    for (TableIdentifier identifier : tables) {
      if (identifier.equals(target)) {
        continue;
      }
      if (matchesSource(catalogWrapper, identifier, tableLocation)) {
        matches.add(identifier);
      }
    }
  }

  private static boolean matchesSource(
      IcebergCatalogWrapper catalogWrapper, TableIdentifier identifier, String tableLocation) {
    Optional<String> currentMeta = catalogWrapper.getTableMetadataLocation(identifier);
    if (currentMeta.isPresent()) {
      return matchesPointer(currentMeta.get(), tableLocation);
    }
    try {
      TableMetadata existing = catalogWrapper.loadTableMetadata(identifier);
      return IcebergTableLocationUtils.overlaps(tableLocation, existing.location());
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping unreadable table metadata during location authorization: {}", identifier, e);
      return false;
    }
  }

  /** Normalizes path strings so {@code file:/} and {@code file:///} compare equal. */
  public static String normalizePath(String path) {
    return IcebergTableLocationUtils.normalize(path);
  }

  private static boolean matchesPointer(String pointer, String location) {
    return IcebergTableLocationUtils.tableLocation(pointer)
        .map(root -> IcebergTableLocationUtils.overlaps(root, location))
        .orElse(false);
  }
}
