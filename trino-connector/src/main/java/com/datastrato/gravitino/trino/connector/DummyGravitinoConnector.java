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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.List;
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
      // call gravitino.system.createCatalog(metalake, catalog, provider, properties, ignoreExist)
      MethodHandle createCatalog =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "createCatalog", String.class, String.class, String.class, boolean.class))
              .bindTo(this);

      List<Procedure.Argument> arguments =
          List.of(
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("PROVIDER", VARCHAR),
              new Procedure.Argument("PROPERTIES", VARCHAR, false, "{}"),
              new Procedure.Argument("IGNORE EXIST", BOOLEAN, false, true));
      Procedure procedure = new Procedure("system", "createCatalog", arguments, createCatalog);
      procedures.add(procedure);

      // call gravitino.system.dropCatalog(metalake, catalog, ignoreNotExist)
      MethodHandle dropCatalog =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "dropCatalog", String.class, boolean.class))
              .bindTo(this);
      arguments =
          List.of(
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("IGNORE NOT EXIST", BOOLEAN, false, true));
      procedure = new Procedure("system", "dropCatalog", arguments, dropCatalog);
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
      String catalogName, String provider, String properties, boolean ignoreExist) {
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
