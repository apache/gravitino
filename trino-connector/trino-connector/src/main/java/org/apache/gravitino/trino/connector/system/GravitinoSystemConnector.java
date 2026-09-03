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
package org.apache.gravitino.trino.connector.system;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

/**
 * GravitinoSystemConnector is primarily used to drive the GravitinoCatalogManager to load catalog
 * connectors managed in the Apache Gravitino server. After users configure the Gravitino connector
 * through Trino catalog configuration, a GravitinoSystemConnector is initially created. And it
 * provides some system tables and stored procedures of Gravitino connector.
 */
// Trino 481 deprecates the split-based createPageSource for removal; it is still the only variant
// available on 435-481. Trino 482 removes it and is handled by a separate version-segment module.
// Trino 482 also deprecated ConnectorPageSource.getMemoryUsage, which SystemTablePageSource
// overrides for older versions.
@SuppressWarnings({"removal", "deprecation"})
public class GravitinoSystemConnector implements Connector {

  private final GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory;

  /**
   * Constructs a new GravitinoSystemConnector.
   *
   * @param gravitinoStoredProcedureFactory the factory for creating stored procedures
   */
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
    return createMetadata();
  }

  protected ConnectorMetadata createMetadata() {
    return new GravitinoSystemConnectorMetadata();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return createSplitManager();
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    return createPageSourceProvider();
  }

  public void shutdown() {}

  protected ConnectorSplitManager createSplitManager() {
    return new SplitManager();
  }

  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new DatasourceProvider();
  }

  /** The transaction handle for Gravitino system connector. */
  public enum TransactionHandle implements ConnectorTransactionHandle {
    /** The singleton instance of the transaction handle. */
    INSTANCE
  }

  /** The datasource provider. */
  public static class DatasourceProvider implements ConnectorPageSourceProvider {

    // Not annotated @Override: this split-based createPageSource is the SPI method up to Trino 481
    // but was removed in Trino 482 (the 482-483 module supplies a MemoryContext-aware variant). It
    // stays here for Trino 435-481.
    public ConnectorPageSource createPageSource(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableHandle table,
        List<ColumnHandle> columns,
        DynamicFilter dynamicFilter) {
      return createPageSource(table);
    }

    /**
     * Loads the system-table page source for the given table handle. Shared by the version-specific
     * {@code createPageSource} SPI overloads (the Trino 482 variant lives in the 482-483 module).
     *
     * @param table the system table handle
     * @return the page source for the system table
     */
    protected ConnectorPageSource createPageSource(ConnectorTableHandle table) {
      SchemaTableName tableName =
          ((GravitinoSystemConnectorMetadata.SystemTableHandle) table).getName();
      return createPageSource(GravitinoSystemTableFactory.loadPageData(tableName));
    }

    protected ConnectorPageSource createPageSource(Page page) {
      throw new TrinoException(NOT_SUPPORTED, "Should be overridden in subclass");
    }
  }

  /** The split manager. */
  public static class SplitManager implements ConnectorSplitManager {

    // Not annotated @Override: this DynamicFilter variant is the SPI method up to Trino 481 but was
    // replaced by the Set<ColumnHandle> variant in Trino 482. Kept for Trino 435-481.
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle connectorTableHandle,
        DynamicFilter dynamicFilter,
        Constraint constraint) {
      return getSplits(connectorTableHandle);
    }

    // Not annotated @Override: this Set<ColumnHandle> variant is the SPI method from Trino 482
    // onward; on Trino 435-481 it is an inert extra method.
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle connectorTableHandle,
        Set<ColumnHandle> dynamicFilterColumns,
        Constraint constraint) {
      return getSplits(connectorTableHandle);
    }

    private ConnectorSplitSource getSplits(ConnectorTableHandle connectorTableHandle) {
      SchemaTableName tableName =
          ((GravitinoSystemConnectorMetadata.SystemTableHandle) connectorTableHandle).getName();
      return new FixedSplitSource(createSplit(tableName));
    }

    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      throw new TrinoException(NOT_SUPPORTED, "Should be overridden in subclass");
    }
  }

  /** The split. */
  public abstract static class Split implements ConnectorSplit {

    protected final SchemaTableName tableName;

    /**
     * Constructs a new Split with the specified table name.
     *
     * @param tableName the table name
     */
    @JsonCreator
    public Split(@JsonProperty("tableName") SchemaTableName tableName) {
      this.tableName = tableName;
    }

    /**
     * Retrieves the table name.
     *
     * @return the table name
     */
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
  }

  /** The system table page source. */
  public abstract static class SystemTablePageSource implements ConnectorPageSource {

    protected boolean isFinished = false;
    protected final Page page;

    /**
     * Constructs a new SystemTablePageSource.
     *
     * @param page the page containing system table data
     */
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

    public Page nextPage() {
      if (isFinished) {
        return null;
      }
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
