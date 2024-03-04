/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DummyGravitinoConnector is primarily used to drive the GravitinoCatalogManager to load catalog
 * connectors managed in the Gravitino server. After users configure the Gravitino connector through
 * Trino catalog configuration, a DummyGravitinoFConnector is initially created. It is just a
 * placeholder.
 */
public class DummyGravitinoConnector implements Connector {

  private final String metalake;
  private final CatalogConnectorManager catalogConnectorManager;

  private final Set<Procedure> procedures = new HashSet<>();

  public DummyGravitinoConnector(String metalake, CatalogConnectorManager catalogConnectorManager) {
    super();
    this.metalake = metalake;
    this.catalogConnectorManager = catalogConnectorManager;

    try {
      // call gravitino.system.create_catalog(catalog, provider, properties, ignore_exist)
      MethodHandle createCatalog =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "createCatalog", String.class, String.class, Map.class, boolean.class))
              .bindTo(this);

      List<Procedure.Argument> arguments =
          List.of(
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("PROVIDER", VARCHAR),
              new Procedure.Argument(
                  "PROPERTIES", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
              new Procedure.Argument("IGNORE_EXIST", BOOLEAN, false, false));
      Procedure procedure = new Procedure("system", "create_catalog", arguments, createCatalog);
      procedures.add(procedure);

      // call gravitino.system.drop_catalog(catalog, ignore_not_exist)
      MethodHandle dropCatalog =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "dropCatalog", String.class, boolean.class))
              .bindTo(this);
      arguments =
          List.of(
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("IGNORE_NOT_EXIST", BOOLEAN, false, false));
      procedure = new Procedure("system", "drop_catalog", arguments, dropCatalog);
      procedures.add(procedure);

    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Failed to initialize gravitino system procedures", e);
    }
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return new ConnectorTransactionHandle() {};
  }

  public void createCatalog(
      String catalogName, String provider, Map<String, String> properties, boolean ignoreExist) {
    catalogConnectorManager.createCatalog(metalake, catalogName, provider, properties, ignoreExist);
  }

  public void dropCatalog(String catalogName, boolean ignoreNotExist) {
    catalogConnectorManager.dropCatalog(metalake, catalogName, ignoreNotExist);
  }

  @Override
  public Set<Procedure> getProcedures() {
    return procedures;
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return new ConnectorMetadata() {
      @Override
      public List<String> listSchemaNames(ConnectorSession session) {
        return List.of("system");
      }
    };
  }
}
