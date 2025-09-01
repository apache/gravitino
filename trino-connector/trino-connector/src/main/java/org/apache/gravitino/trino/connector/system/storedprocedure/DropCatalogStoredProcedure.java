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

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stored procedure implementation for dropping an existing catalog in Gravitino.
 *
 * <p>This procedure allows dropping a catalog with optional ignoreNotExist flag.
 */
public class DropCatalogStoredProcedure extends GravitinoStoredProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(DropCatalogStoredProcedure.class);

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  /**
   * Constructs a new DropCatalogStoredProcedure.
   *
   * @param catalogConnectorManager the catalog connector manager
   * @param metalake the metalake name
   */
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
    return new Procedure(
        GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME, "drop_catalog", arguments, dropCatalog);
  }

  /**
   * Drops the specified catalog.
   *
   * @param catalogName the name of the catalog to drop
   * @param ignoreNotExist whether to ignore if the catalog does not exist
   * @throws TrinoException if the catalog does not exist and ignoreNotExist is false
   */
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
            GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS,
            "Catalog " + NameIdentifier.of(metalake, catalogName) + " not exists.");
      }
      catalogConnector.getMetalake().dropCatalog(catalogName, true);

      catalogConnectorManager.loadMetalakeSync();

      if (catalogConnectorManager.catalogConnectorExist(
          catalogConnectorManager.getTrinoCatalogName(metalake, catalogName))) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_OPERATION_FAILED,
            "Drop catalog failed due to the reloading process fails");
      }

      LOG.info("Drop catalog {} in metalake {} successfully.", catalogName, metalake);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS,
          "Metalake " + metalake + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION,
          "Drop catalog failed. " + (StringUtils.isEmpty(e.getMessage()) ? "" : e.getMessage()),
          e);
    }
  }
}
