/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system;

import com.datastrato.gravitino.trino.connector.system.storedprocdure.GravitinoStoredProcedureFactory;
import com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * GravitinoSystemConnector is primarily used to drive the GravitinoCatalogManager to load catalog
 * connectors managed in the Gravitino server. After users configure the Gravitino connector through
 * Trino catalog configuration, a GravitinoSystemConnector is initially created. And it provides
 * some system tables and stored procedures of Gravitino connector.
 */
public class GravitinoSystemConnector implements Connector {

  private final GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory;

  public GravitinoSystemConnector(GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory) {
    this.gravitinoStoredProcedureFactory = gravitinoStoredProcedureFactory;
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return TransactionHandle.INSTANCE;
  }

  @Override
  public Set<Procedure> getProcedures() {
    return gravitinoStoredProcedureFactory.getStoredProcedures();
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return new GravitinoSystemConnectorMetadata();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return new SplitManager();
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

      SchemaTableName tableName =
          ((GravitinoSystemConnectorMetadata.SystemTableHandle) table).getName();
      return new SystemTablePageSource(GravitinoSystemTableFactory.loadPageData(tableName));
    }
  }

  public static class SplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle connectorTableHandle,
        DynamicFilter dynamicFilter,
        Constraint constraint) {

      SchemaTableName tableName =
          ((GravitinoSystemConnectorMetadata.SystemTableHandle) connectorTableHandle).getName();
      return new FixedSplitSource(new Split(tableName));
    }
  }

  public static class Split implements ConnectorSplit {
    private final SchemaTableName tableName;

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
      return Collections.emptyList();
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
