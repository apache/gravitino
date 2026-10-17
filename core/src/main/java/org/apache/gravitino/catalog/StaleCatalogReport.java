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
package org.apache.gravitino.catalog;

import java.util.List;

/**
 * The stale-registration reconcile result for one catalog.
 *
 * <p>A report is the unit of information a reconcile run produces per catalog. It carries
 * everything a caller (the periodic task log, or a future REST surface) needs to inspect stale
 * registrations before any removal is attempted.
 */
public class StaleCatalogReport {

  private final String metalake;

  private final String catalog;

  private final boolean catalogUnreachable;

  private final List<StaleSchemaEntry> staleSchemas;

  private StaleCatalogReport(
      String metalake, String catalog, boolean catalogUnreachable, List<StaleSchemaEntry> entries) {
    this.metalake = metalake;
    this.catalog = catalog;
    this.catalogUnreachable = catalogUnreachable;
    this.staleSchemas = List.copyOf(entries);
  }

  /**
   * Creates an empty report for a catalog that was scanned and found clean, or that is out of scope
   * (for example a catalog that manages storage, or one without schema operations).
   *
   * @param metalake the metalake name
   * @param catalog the catalog name
   * @return an empty report
   */
  public static StaleCatalogReport empty(String metalake, String catalog) {
    return new StaleCatalogReport(metalake, catalog, false, List.of());
  }

  /**
   * Creates a report marking the catalog unreachable in this round. An unreachable catalog has no
   * removals and no stale entries: absence could not be confirmed against its source.
   *
   * @param metalake the metalake name
   * @param catalog the catalog name
   * @return an unreachable report
   */
  public static StaleCatalogReport unreachable(String metalake, String catalog) {
    return new StaleCatalogReport(metalake, catalog, true, List.of());
  }

  /**
   * Creates a report carrying the detected stale schema entries.
   *
   * @param metalake the metalake name
   * @param catalog the catalog name
   * @param entries the stale schema entries detected in this round
   * @return the report
   */
  public static StaleCatalogReport of(
      String metalake, String catalog, List<StaleSchemaEntry> entries) {
    return new StaleCatalogReport(metalake, catalog, false, entries);
  }

  /**
   * Returns the metalake name of the scanned catalog.
   *
   * @return the metalake name
   */
  public String metalake() {
    return metalake;
  }

  /**
   * Returns the name of the scanned catalog.
   *
   * @return the catalog name
   */
  public String catalog() {
    return catalog;
  }

  /**
   * Returns whether the catalog was unreachable in this round. Unreachable catalogs are skipped
   * entirely: their registrations are neither reported stale nor removed.
   *
   * @return whether the catalog was unreachable
   */
  public boolean catalogUnreachable() {
    return catalogUnreachable;
  }

  /**
   * Returns the stale schema entries of this catalog. For hierarchical namespaces the list carries
   * the outermost stale schemas only: descendants of a stale schema are carried along by its
   * cascading removal, so they are not listed separately.
   *
   * @return an unmodifiable list of stale schema entries
   */
  public List<StaleSchemaEntry> staleSchemas() {
    return staleSchemas;
  }
}
