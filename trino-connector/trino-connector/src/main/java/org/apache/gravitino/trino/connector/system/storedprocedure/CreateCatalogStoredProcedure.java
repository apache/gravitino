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
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stored procedure implementation for creating a new catalog in Gravitino.
 *
 * <p>This procedure allows creating a new catalog with specified properties and provider.
 */
public class CreateCatalogStoredProcedure extends GravitinoStoredProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(CreateCatalogStoredProcedure.class);

  private final CatalogConnectorManager catalogConnectorManager;
  private final String metalake;

  /**
   * Constructs a new CreateCatalogStoredProcedure.
   *
   * @param catalogConnectorManager the catalog connector manager
   * @param metalake the metalake name
   */
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

    return new Procedure(
        GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME, "create_catalog", arguments, createCatalog);
  }

  /**
   * Creates a new catalog in Gravitino.
   *
   * @param catalogName the name of the catalog to create
   * @param provider the provider of the catalog
   * @param properties the properties of the catalog
   * @param ignoreExist whether to ignore if the catalog already exists
   * @throws TrinoException if the catalog already exists and ignoreExist is false
   */
  public void createCatalog(
      String catalogName, String provider, Map<String, String> properties, boolean ignoreExist) {
    boolean exists =
        catalogConnectorManager.catalogConnectorExist(
            catalogConnectorManager.getTrinoCatalogName(metalake, catalogName));
    if (exists) {
      if (!ignoreExist) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS,
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
            GravitinoErrorCode.GRAVITINO_OPERATION_FAILED,
            "Create catalog failed due to the loading process fails");
      }

      LOG.info("Create catalog {} in metalake {} successfully.", catalogName, metalake);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS,
          "Metalake " + metalake + " not exists.");
    } catch (CatalogAlreadyExistsException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS,
          "Catalog " + NameIdentifier.of(metalake, catalogName) + " already exists in the server.");
    } catch (Exception e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION,
          "Create catalog failed. " + (StringUtils.isEmpty(e.getMessage()) ? "" : e.getMessage()),
          e);
    }
  }
}
