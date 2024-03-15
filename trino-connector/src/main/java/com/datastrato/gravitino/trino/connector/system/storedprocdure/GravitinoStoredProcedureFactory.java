/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.storedprocdure;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    procedures.add(new AlterCatalogPropertiesStoredProcedure(catalogConnectorManager, metalake));
  }

  public Set<Procedure> getStoredProcedures() {
    return procedures.stream()
        .map(
            v -> {
              try {
                return v.createStoredProcedure();
              } catch (Exception e) {
                throw new TrinoException(
                    GRAVITINO_UNSUPPORTED_OPERATION,
                    "Failed to initialize gravitino system procedures",
                    e);
              }
            })
        .collect(Collectors.toSet());
  }
}
