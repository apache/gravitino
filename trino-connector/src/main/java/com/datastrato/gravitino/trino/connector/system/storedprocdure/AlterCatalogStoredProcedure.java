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
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterCatalogStoredProcedure extends GravitinoStoredProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(AlterCatalogStoredProcedure.class);

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
                    0, Optional.empty(), new int[1], VARCHAR.createBlockBuilder(null, 1).build())));
    return new Procedure(SYSTEM_TABLE_SCHEMA_NAME, "alter_catalog", arguments, dropCatalog);
  }

  public void alterCatalog(
      String catalogName, Map<String, String> setProperties, List<String> removeProperties) {
    try {
      CatalogConnectorContext catalogConnectorContext =
          catalogConnectorManager.getCatalogConnector(
              catalogConnectorManager.getTrinoCatalogName(metalake, catalogName));
      GravitinoCatalog oldCatalog = catalogConnectorContext.getCatalog();

      List<CatalogChange> changes = new ArrayList<>();
      setProperties.forEach(
          (key, value) -> {
            // Skip the no changed attributes
            boolean matched =
                oldCatalog.getProperties().entrySet().stream()
                    .anyMatch(oe -> oe.getKey().equals(key) && oe.getValue().equals(value));
            if (!matched) {
              changes.add(CatalogChange.setProperty(key, value));
            }
          });

      removeProperties.forEach(
          key -> {
            if (oldCatalog.getProperties().containsKey(key)) {
              changes.add(CatalogChange.removeProperty(key));
            }
          });

      if (changes.isEmpty()) {
        return;
      }

      catalogConnectorContext
          .getMetalake()
          .alterCatalog(catalogName, changes.toArray(changes.toArray(new CatalogChange[0])));

      catalogConnectorManager.loadMetalakeSync();
      catalogConnectorContext =
          catalogConnectorManager.getCatalogConnector(
              catalogConnectorManager.getTrinoCatalogName(metalake, catalogName));
      if (catalogConnectorContext == null
          || catalogConnectorContext.getCatalog().getLastModifiedTime()
              == oldCatalog.getLastModifiedTime()) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED, "Update catalog failed due to the reloading process fails");
      }
      LOG.info("Alter catalog {} in metalake {} successfully.", catalogName, metalake);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalake + " not exists.");
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(
          GRAVITINO_CATALOG_NOT_EXISTS,
          "Catalog " + NameIdentifier.of(metalake, catalogName) + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "alter catalog failed. " + e.getMessage(), e);
    }
  }
}
