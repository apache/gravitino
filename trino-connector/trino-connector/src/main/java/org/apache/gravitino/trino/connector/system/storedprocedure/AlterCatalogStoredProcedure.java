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

import static io.trino.spi.type.VarcharType.VARCHAR;

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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stored procedure implementation for altering an existing catalog in Gravitino.
 *
 * <p>This procedure allows setting and removing catalog properties dynamically.
 */
public class AlterCatalogStoredProcedure extends GravitinoStoredProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(AlterCatalogStoredProcedure.class);

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  /**
   * Constructs a new AlterCatalogStoredProcedure.
   *
   * @param catalogConnectorManager the catalog connector manager
   * @param metalake the metalake name
   */
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
    return new Procedure(
        GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME, "alter_catalog", arguments, dropCatalog);
  }

  /**
   * Alters the specified catalog by setting or removing properties.
   *
   * @param catalogName the name of the catalog to alter
   * @param setProperties the properties to set
   * @param removeProperties the properties to remove
   * @throws TrinoException if the catalog does not exist or the operation fails
   */
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
            GravitinoErrorCode.GRAVITINO_OPERATION_FAILED,
            "Update catalog failed due to the reloading process fails");
      }
      LOG.info("Alter catalog {} in metalake {} successfully.", catalogName, metalake);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS,
          "Metalake " + metalake + " not exists.");
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS,
          "Catalog " + NameIdentifier.of(metalake, catalogName) + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION,
          "alter catalog failed. " + (StringUtils.isEmpty(e.getMessage()) ? "" : e.getMessage()),
          e);
    }
  }
}
