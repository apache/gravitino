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
package org.apache.gravitino.trino.connector.system.storedprocdure;

import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

/** This class managed all the stored procedures */
public class GravitinoStoredProcedureFactory {

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  public final List<GravitinoStoredProcedure> procedures = new ArrayList<>();

  public GravitinoStoredProcedureFactory(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;

    registerStoredProcedure();
  }

  /** Register all the stored procedures * */
  private void registerStoredProcedure() {
    procedures.add(new CreateCatalogStoredProcedure(catalogConnectorManager, metalake));
    procedures.add(new DropCatalogStoredProcedure(catalogConnectorManager, metalake));
    procedures.add(new AlterCatalogStoredProcedure(catalogConnectorManager, metalake));
  }

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
