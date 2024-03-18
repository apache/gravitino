/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.storedprocdure;

import static com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AlterCatalogStoredProcedure extends GravitinoStoredProcedure {

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  public AlterCatalogStoredProcedure(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

  @Override
  public Procedure createStoredProcedure() throws NoSuchMethodException, IllegalAccessException {
    // call gravitino.system.alter_catalog(catalogName, set_properties, remove_properties

    MethodHandle dropCatalog =
        MethodHandles.lookup()
            .unreflect(
                AlterCatalogStoredProcedure.class.getMethod(
                    "alterCatalog", String.class, Map.class, List.class))
            .bindTo(this);
    List<Procedure.Argument> arguments =
        List.of(
            new Procedure.Argument("CATALOG", VARCHAR),
            new Procedure.Argument(
                "SET_PROPERTIES", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
            new Procedure.Argument(
                "REMOVE_PROPERTIES",
                new ArrayType(VARCHAR),
                false,
                ArrayBlock.fromElementBlock(
                    0, Optional.empty(), new int[1], VARCHAR.createBlockBuilder(null, 1))));
    return new Procedure(SYSTEM_TABLE_SCHEMA_NAME, "alter_catalog", arguments, dropCatalog);
  }

  public void alterCatalog(
      String catalogName, Map<String, String> set_properties, List<String> remove_properties) {
    catalogConnectorManager.alterCatalog(metalake, catalogName, set_properties, remove_properties);
  }
}
