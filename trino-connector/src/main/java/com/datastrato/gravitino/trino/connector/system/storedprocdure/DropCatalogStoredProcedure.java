/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.storedprocdure;

import static com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.procedure.Procedure;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

public class DropCatalogStoredProcedure extends GravitinoStoredProcedure {

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  public DropCatalogStoredProcedure(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

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
    catalogConnectorManager.dropCatalog(metalake, catalogName, ignoreNotExist);
  }
}
