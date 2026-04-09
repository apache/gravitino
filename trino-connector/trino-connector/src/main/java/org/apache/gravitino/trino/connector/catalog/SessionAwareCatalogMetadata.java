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
package org.apache.gravitino.trino.connector.catalog;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoSchema;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.apache.gravitino.trino.connector.security.TrinoSessionContext;

/**
 * A session-aware facade over {@link CatalogConnectorMetadata} that automatically applies and
 * clears Trino session credentials around every Gravitino API call.
 *
 * <p>When {@code sessionContext} is {@code null} (auth forwarding is not configured), calls are
 * forwarded to the delegate without any session management overhead.
 *
 * @since 1.3.0
 */
public class SessionAwareCatalogMetadata {

  private final CatalogConnectorMetadata delegate;
  @Nullable private final TrinoSessionContext sessionContext;

  /**
   * Constructs a new SessionAwareCatalogMetadata.
   *
   * @param delegate the underlying metadata implementation
   * @param sessionContext the session context used to forward Trino credentials, or {@code null}
   *     when forwarding is not configured
   */
  public SessionAwareCatalogMetadata(
      CatalogConnectorMetadata delegate, @Nullable TrinoSessionContext sessionContext) {
    this.delegate = delegate;
    this.sessionContext = sessionContext;
  }

  public List<String> listSchemaNames(ConnectorSession session) {
    return doAs(session, delegate::listSchemaNames);
  }

  public GravitinoSchema getSchema(ConnectorSession session, String schemaName) {
    return doAs(session, () -> delegate.getSchema(schemaName));
  }

  public GravitinoTable getTable(ConnectorSession session, String schemaName, String tableName) {
    return doAs(session, () -> delegate.getTable(schemaName, tableName));
  }

  public List<String> listTables(ConnectorSession session, String schemaName) {
    return doAs(session, () -> delegate.listTables(schemaName));
  }

  public boolean tableExists(ConnectorSession session, String schemaName, String tableName) {
    return doAs(session, () -> delegate.tableExists(schemaName, tableName));
  }

  public void createTable(ConnectorSession session, GravitinoTable table, boolean ignoreExisting) {
    doAs(session, () -> delegate.createTable(table, ignoreExisting));
  }

  public void createSchema(ConnectorSession session, GravitinoSchema schema) {
    doAs(session, () -> delegate.createSchema(schema));
  }

  public void dropSchema(ConnectorSession session, String schemaName, boolean cascade) {
    doAs(session, () -> delegate.dropSchema(schemaName, cascade));
  }

  public void dropTable(ConnectorSession session, SchemaTableName tableName) {
    doAs(session, () -> delegate.dropTable(tableName));
  }

  public void renameSchema(ConnectorSession session, String source, String target) {
    doAs(session, () -> delegate.renameSchema(source, target));
  }

  public void renameTable(
      ConnectorSession session, SchemaTableName oldTableName, SchemaTableName newTableName) {
    doAs(session, () -> delegate.renameTable(oldTableName, newTableName));
  }

  public void setTableComment(
      ConnectorSession session, SchemaTableName schemaTableName, String comment) {
    doAs(session, () -> delegate.setTableComment(schemaTableName, comment));
  }

  public void setTableProperties(
      ConnectorSession session, SchemaTableName schemaTableName, Map<String, String> properties) {
    doAs(session, () -> delegate.setTableProperties(schemaTableName, properties));
  }

  public void addColumn(
      ConnectorSession session, SchemaTableName schemaTableName, GravitinoColumn column) {
    doAs(session, () -> delegate.addColumn(schemaTableName, column));
  }

  public void addColumn(
      ConnectorSession session,
      SchemaTableName schemaTableName,
      GravitinoColumn column,
      TableChange.ColumnPosition position) {
    doAs(session, () -> delegate.addColumn(schemaTableName, column, position));
  }

  public void dropColumn(
      ConnectorSession session, SchemaTableName schemaTableName, String columnName) {
    doAs(session, () -> delegate.dropColumn(schemaTableName, columnName));
  }

  public void renameColumn(
      ConnectorSession session, SchemaTableName schemaTableName, String columnName, String target) {
    doAs(session, () -> delegate.renameColumn(schemaTableName, columnName, target));
  }

  public void setColumnComment(
      ConnectorSession session,
      SchemaTableName schemaTableName,
      String columnName,
      String comment) {
    doAs(session, () -> delegate.setColumnComment(schemaTableName, columnName, comment));
  }

  public void setColumnType(
      ConnectorSession session, SchemaTableName schemaTableName, String columnName, Type type) {
    doAs(session, () -> delegate.setColumnType(schemaTableName, columnName, type));
  }

  private <T> T doAs(ConnectorSession session, Supplier<T> action) {
    if (sessionContext != null) {
      sessionContext.applySession(session);
    }
    try {
      return action.get();
    } finally {
      if (sessionContext != null) {
        sessionContext.clearSession();
      }
    }
  }

  private void doAs(ConnectorSession session, Runnable action) {
    if (sessionContext != null) {
      sessionContext.applySession(session);
    }
    try {
      action.run();
    } finally {
      if (sessionContext != null) {
        sessionContext.clearSession();
      }
    }
  }
}
