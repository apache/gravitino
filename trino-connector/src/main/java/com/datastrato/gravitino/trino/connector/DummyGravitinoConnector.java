/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
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

  private final CatalogConnectorManager catalogConnectorManager;

  private static Set<Procedure> procedures = new HashSet<>();

  public DummyGravitinoConnector(CatalogConnectorManager catalogConnectorManager) {
    super();
    this.catalogConnectorManager = catalogConnectorManager;

    try {
      MethodHandle createCatalog =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "createCatalog",
                      String.class,
                      String.class,
                      String.class,
                      String.class,
                      boolean.class))
              .bindTo(this);

      List<Procedure.Argument> arguments =
          List.of(
              new Procedure.Argument("METALAKE", VARCHAR),
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("PROVIDER", VARCHAR),
              new Procedure.Argument("PROPERTIES", VARCHAR),
              new Procedure.Argument("IGNORE EXIST", BOOLEAN, false, true));
      Procedure procedure = new Procedure("system", "createCatalog", arguments, createCatalog);
      procedures.add(procedure);

      MethodHandle createMetalake =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "createMetalake", String.class, boolean.class))
              .bindTo(this);
      arguments =
          List.of(
              new Procedure.Argument("METALAKE", VARCHAR),
              new Procedure.Argument("IGNORE EXIST", BOOLEAN, false, true));
      procedure = new Procedure("system", "createMetalake", arguments, createMetalake);
      procedures.add(procedure);

      MethodHandle dropCatalog =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "dropCatalog", String.class, String.class, boolean.class))
              .bindTo(this);
      arguments =
          List.of(
              new Procedure.Argument("METALAKE", VARCHAR),
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("IGNORE NOT EXIST", BOOLEAN, false, true));
      procedure = new Procedure("system", "dropCatalog", arguments, dropCatalog);
      procedures.add(procedure);

      MethodHandle dropMetalake =
          MethodHandles.lookup()
              .unreflect(
                  DummyGravitinoConnector.class.getMethod(
                      "dropMetalake", String.class, boolean.class))
              .bindTo(this);
      arguments =
          List.of(
              new Procedure.Argument("METALAKE", VARCHAR),
              new Procedure.Argument("IGNORE NOT EXIST", BOOLEAN, false, true));
      procedure = new Procedure("system", "dropMetalake", arguments, dropMetalake);
      procedures.add(procedure);

    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return new ConnectorTransactionHandle() {};
  }

  public void createCatalog(
      String metalakeName,
      String catalogName,
      String provider,
      String properties,
      boolean ignoreExist) {
    catalogConnectorManager.createCatalog(
        metalakeName, catalogName, provider, properties, ignoreExist);
  }

  public void createMetalake(String name, boolean ignoreExist) {
    catalogConnectorManager.createMetalake(name, ignoreExist);
  }

  public void dropCatalog(String metalakeName, String catalogName, boolean ignoreNotExist) {
    catalogConnectorManager.dropCatalog(metalakeName, catalogName, ignoreNotExist);
  }

  public void dropMetalake(String name, boolean ignoreNotExist) {
    catalogConnectorManager.dropMetalake(name, ignoreNotExist);
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
