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
package org.apache.gravitino.trino.connector.system.storedprocedure;

import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

/** This class managed all the stored procedures */
public class GravitinoStoredProcedureFactory {

  private final CatalogConnectorManager catalogConnectorManager;
  @Nullable private final String configuredMetalake;

  /** List of all registered Gravitino stored procedures */
  public final List<GravitinoStoredProcedure> procedures = new ArrayList<>();

  /**
   * Constructs a new GravitinoStoredProcedureFactory.
   *
   * @param catalogConnectorManager the catalog connector manager
   * @param configuredMetalake the metalake name, or null when the connector is not configured with
   *     one
   */
  public GravitinoStoredProcedureFactory(
      CatalogConnectorManager catalogConnectorManager, @Nullable String configuredMetalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.configuredMetalake = configuredMetalake;

    registerStoredProcedure();
  }

  /** Register all the stored procedures * */
  private void registerStoredProcedure() {
    procedures.add(new CreateCatalogStoredProcedure(catalogConnectorManager, configuredMetalake));
    procedures.add(new DropCatalogStoredProcedure(catalogConnectorManager, configuredMetalake));
    procedures.add(new AlterCatalogStoredProcedure(catalogConnectorManager, configuredMetalake));
  }

  /**
   * Gets all registered stored procedures.
   *
   * @return a set of all stored procedures
   * @throws TrinoException if failed to initialize any stored procedure
   */
  public Set<Procedure> getStoredProcedures() {
    return procedures.stream()
        .map(
            v -> {
              try {
                return v.createStoredProcedure();
              } catch (Exception e) {
                throw new TrinoException(
                    GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION,
                    "Failed to initialize Gravitino system procedures",
                    e);
              }
            })
        .collect(Collectors.toSet());
  }
}
