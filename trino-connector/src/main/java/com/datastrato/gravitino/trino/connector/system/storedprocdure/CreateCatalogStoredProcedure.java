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
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

public class CreateCatalogStoredProcedure extends GravitinoStoredProcedure {

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  public CreateCatalogStoredProcedure(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

  public Procedure createStoredProcedure() throws NoSuchMethodException, IllegalAccessException {
    // call gravitino.system.create_catalog(catalog, provider, properties, ignore_exist)
    MethodHandle createCatalog =
        MethodHandles.lookup()
            .unreflect(
                CreateCatalogStoredProcedure.class.getMethod(
                    "createCatalog", String.class, String.class, Map.class, boolean.class))
            .bindTo(this);

    List<Procedure.Argument> arguments =
        List.of(
            new Procedure.Argument("CATALOG", VARCHAR),
            new Procedure.Argument("PROVIDER", VARCHAR),
            new Procedure.Argument(
                "PROPERTIES", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
            new Procedure.Argument("IGNORE_EXIST", BOOLEAN, false, false));

    return new Procedure(SYSTEM_TABLE_SCHEMA_NAME, "create_catalog", arguments, createCatalog);
  }

  public void createCatalog(
      String catalogName, String provider, Map<String, String> properties, boolean ignoreExist) {
    catalogConnectorManager.createCatalog(metalake, catalogName, provider, properties, ignoreExist);
  }
}
