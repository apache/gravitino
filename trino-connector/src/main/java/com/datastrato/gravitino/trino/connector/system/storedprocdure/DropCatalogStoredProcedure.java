/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.storedprocdure;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_OPERATION_FAILED;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;
import static com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropCatalogStoredProcedure extends GravitinoStoredProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(DropCatalogStoredProcedure.class);

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  public DropCatalogStoredProcedure(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

  @Override
  public Procedure createStoredProcedure() throws NoSuchMethodException, IllegalAccessException {
    // call gravitino.system.drop_catalog(catalog, ignore_not_exist)
    MethodHandle dropCatalog =
        MethodHandles.lookup()
            .unreflect(
                DropCatalogStoredProcedure.class.getMethod(
                    "dropCatalog", String.class, boolean.class))
            .bindTo(this);
    List<Procedure.Argument> arguments =
        List.of(
            new Procedure.Argument("CATALOG", VARCHAR),
            new Procedure.Argument("IGNORE_NOT_EXIST", BOOLEAN, false, false));
    return new Procedure(SYSTEM_TABLE_SCHEMA_NAME, "drop_catalog", arguments, dropCatalog);
  }

  public void dropCatalog(String catalogName, boolean ignoreNotExist) {
    try {
      CatalogConnectorContext catalogConnector =
          catalogConnectorManager.getCatalogConnector(
              catalogConnectorManager.getTrinoCatalogName(metalake, catalogName));
      if (catalogConnector == null) {
        if (ignoreNotExist) {
          return;
        }

        throw new TrinoException(
            GRAVITINO_CATALOG_NOT_EXISTS,
            "Catalog " + NameIdentifier.of(metalake, catalogName) + " not exists.");
      }
      boolean dropped = catalogConnector.getMetalake().dropCatalog(catalogName);
      if (!dropped) {
        throw new TrinoException(
            GRAVITINO_UNSUPPORTED_OPERATION, "Failed to drop no empty catalog " + catalogName);
      }

      catalogConnectorManager.loadMetalakeSync();

      if (catalogConnectorManager.catalogConnectorExist(
          catalogConnectorManager.getTrinoCatalogName(metalake, catalogName))) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED, "Drop catalog failed due to the reloading process fails");
      }

      LOG.info("Drop catalog {} in metalake {} successfully.", catalogName, metalake);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalake + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Drop catalog failed. " + e.getMessage(), e);
    }
  }
}
