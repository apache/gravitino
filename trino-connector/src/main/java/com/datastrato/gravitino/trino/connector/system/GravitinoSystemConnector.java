/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;
import static com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTable.SYSTEM_TABLE_SCHEMA_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GravitinoSystemConnector is primarily used to drive the GravitinoCatalogManager to load catalog
 * connectors managed in the Gravitino server. After users configure the Gravitino connector through
 * Trino catalog configuration, a DummyGravitinoFConnector is initially created. And it provides
 * some system tables and stored procedures of Gravitino connector.
 */
public class GravitinoSystemConnector implements Connector {

  private final String metalake;
  private final CatalogConnectorManager catalogConnectorManager;

  private final Set<Procedure> procedures = new HashSet<>();

  public GravitinoSystemConnector(
      String metalake, CatalogConnectorManager catalogConnectorManager) {
    this.metalake = metalake;
    this.catalogConnectorManager = catalogConnectorManager;

    try {
      // call gravitino.system.create_catalog(catalog, provider, properties, ignore_exist)
      MethodHandle createCatalog =
          MethodHandles.lookup()
              .unreflect(
                  GravitinoSystemConnector.class.getMethod(
                      "createCatalog", String.class, String.class, Map.class, boolean.class))
              .bindTo(this);

      List<Procedure.Argument> arguments =
          List.of(
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("PROVIDER", VARCHAR),
              new Procedure.Argument(
                  "PROPERTIES", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
              new Procedure.Argument("IGNORE_EXIST", BOOLEAN, false, false));
      Procedure procedure =
          new Procedure(SYSTEM_TABLE_SCHEMA_NAME, "create_catalog", arguments, createCatalog);
      procedures.add(procedure);

      // call gravitino.system.drop_catalog(catalog, ignore_not_exist)
      MethodHandle dropCatalog =
          MethodHandles.lookup()
              .unreflect(
                  GravitinoSystemConnector.class.getMethod(
                      "dropCatalog", String.class, boolean.class))
              .bindTo(this);
      arguments =
          List.of(
              new Procedure.Argument("CATALOG", VARCHAR),
              new Procedure.Argument("IGNORE_NOT_EXIST", BOOLEAN, false, false));
      procedure = new Procedure(SYSTEM_TABLE_SCHEMA_NAME, "drop_catalog", arguments, dropCatalog);
      procedures.add(procedure);

    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Failed to initialize gravitino system procedures", e);
    }
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return TransactionHandle.INSTANCE;
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
    return new GravitinoSystemConnectorMetadata();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return new SplitManger();
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    return new DatasourceProvider();
  }

  public enum TransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
  }

  public static class DatasourceProvider implements ConnectorPageSourceProvider {

    @Override
    public ConnectorPageSource createPageSource(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableHandle table,
        List<ColumnHandle> columns,
        DynamicFilter dynamicFilter) {

      SchemaTableName tableName = ((GravitinoSystemConnectorMetadata.SystemTableHandle) table).name;
      return new SystemTablePageSource(GravitinoSystemTableFactory.loadPageData(tableName));
    }
  }

  public static class SplitManger implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle connectorTableHandle,
        DynamicFilter dynamicFilter,
        Constraint constraint) {
      SchemaTableName tableName =
          ((GravitinoSystemConnectorMetadata.SystemTableHandle) connectorTableHandle).name;
      return new FixedSplitSource(new Split(tableName));
    }
  }

  public static class Split implements ConnectorSplit {
    SchemaTableName tableName;

    @JsonCreator
    public Split(@JsonProperty("tableName") SchemaTableName tableName) {
      this.tableName = tableName;
    }

    @JsonProperty
    public SchemaTableName getTableName() {
      return tableName;
    }

    @Override
    public boolean isRemotelyAccessible() {
      return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
      return List.of(HostAddress.fromParts("127.0.0.1", 8080));
    }

    @Override
    public Object getInfo() {
      return this;
    }
  }

  public static class SystemTablePageSource implements ConnectorPageSource {

    private boolean isFinished = false;
    private final Page page;

    public SystemTablePageSource(Page page) {
      this.page = page;
    }

    @Override
    public long getCompletedBytes() {
      return 0;
    }

    @Override
    public long getReadTimeNanos() {
      return 0;
    }

    @Override
    public boolean isFinished() {
      return isFinished;
    }

    @Override
    public Page getNextPage() {
      isFinished = true;
      return page;
    }

    @Override
    public long getMemoryUsage() {
      return 0;
    }

    @Override
    public void close() throws IOException {}
  }
}
