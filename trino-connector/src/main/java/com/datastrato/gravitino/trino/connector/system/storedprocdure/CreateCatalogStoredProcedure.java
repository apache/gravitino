/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.storedprocdure;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_OPERATION_FAILED;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;
import static com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateCatalogStoredProcedure extends GravitinoStoredProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(CreateCatalogStoredProcedure.class);

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  public CreateCatalogStoredProcedure(
      CatalogConnectorManager catalogConnectorManager, String metalake) {
    this.catalogConnectorManager = catalogConnectorManager;
    this.metalake = metalake;
  }

  @Override
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
    boolean exists =
        catalogConnectorManager.catalogConnectorExist(
            catalogConnectorManager.getTrinoCatalogName(metalake, catalogName));
    if (exists) {
      if (!ignoreExist) {
        throw new TrinoException(
            GRAVITINO_CATALOG_ALREADY_EXISTS,
            String.format("Catalog %s already exists.", NameIdentifier.of(metalake, catalogName)));
      }
      return;
    }

    try {
      catalogConnectorManager
          .getMetalake(metalake)
          .createCatalog(
              catalogName, Catalog.Type.RELATIONAL, provider, "Trino created", properties);

      catalogConnectorManager.loadMetalakeSync();
      if (!catalogConnectorManager.catalogConnectorExist(
          catalogConnectorManager.getTrinoCatalogName(metalake, catalogName))) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED, "Create catalog failed due to the loading process fails");
      }

      LOG.info("Create catalog {} in metalake {} successfully.", catalogName, metalake);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalake + " not exists.");
    } catch (CatalogAlreadyExistsException e) {
      throw new TrinoException(
          GRAVITINO_CATALOG_ALREADY_EXISTS,
          "Catalog " + NameIdentifier.of(metalake, catalogName) + " already exists in the server.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Create catalog failed. " + e.getMessage(), e);
    }
  }
}
