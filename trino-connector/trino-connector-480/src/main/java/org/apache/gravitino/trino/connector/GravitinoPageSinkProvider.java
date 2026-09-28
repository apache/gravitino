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
package org.apache.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import java.util.Optional;

/**
 * This class provides a ConnectorPageSink for Trino to write data to internal connector.
 *
 * <p>This module-local copy shadows the shared non-credential-shaped class, which is excluded from
 * this module's source set. Trino 480 added the {@code Optional<ConnectorTableCredentials>} {@code
 * createPageSink}/{@code createMergeSink} variants used by the engine from Trino 481 onward; both
 * the legacy and the credential-aware variants are implemented with direct delegation here.
 */
@SuppressWarnings("removal")
public class GravitinoPageSinkProvider implements ConnectorPageSinkProvider {

  ConnectorPageSinkProvider pageSinkProvider;

  /**
   * Constructs a new GravitinoPageSinkProvider with the specified page sink provider.
   *
   * @param pageSinkProvider the internal connector page sink provider
   */
  public GravitinoPageSinkProvider(ConnectorPageSinkProvider pageSinkProvider) {
    this.pageSinkProvider = pageSinkProvider;
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      ConnectorPageSinkId pageSinkId) {
    // GravitinoOutputTableHandle wraps a ConnectorInsertTableHandle internally,
    // so delegate to the insert-path createPageSink
    ConnectorInsertTableHandle insertHandle =
        ((GravitinoOutputTableHandle) outputTableHandle).getInternalHandle();
    return createInsertPageSink(transactionHandle, session, insertHandle, pageSinkId);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return createInsertPageSink(
        transactionHandle, session, GravitinoHandle.unWrap(insertTableHandle), pageSinkId);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorTableExecuteHandle tableExecuteHandle,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        GravitinoHandle.unWrap(tableExecuteHandle),
        pageSinkId);
  }

  @Override
  public ConnectorMergeSink createMergeSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorMergeTableHandle mergeHandle,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createMergeSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        GravitinoHandle.unWrap(mergeHandle),
        pageSinkId);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    // GravitinoOutputTableHandle wraps a ConnectorInsertTableHandle internally, so delegate to the
    // insert-path createPageSink.
    ConnectorInsertTableHandle insertHandle =
        ((GravitinoOutputTableHandle) outputTableHandle).getInternalHandle();
    return createInsertPageSink(
        transactionHandle, session, insertHandle, tableCredentials, pageSinkId);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return createInsertPageSink(
        transactionHandle,
        session,
        GravitinoHandle.unWrap(insertTableHandle),
        tableCredentials,
        pageSinkId);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorTableExecuteHandle tableExecuteHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        GravitinoHandle.unWrap(tableExecuteHandle),
        tableCredentials,
        pageSinkId);
  }

  @Override
  public ConnectorMergeSink createMergeSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorMergeTableHandle mergeHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createMergeSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        GravitinoHandle.unWrap(mergeHandle),
        tableCredentials,
        pageSinkId);
  }

  private ConnectorPageSink createInsertPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle), session, insertTableHandle, pageSinkId);
  }

  private ConnectorPageSink createInsertPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        insertTableHandle,
        tableCredentials,
        pageSinkId);
  }
}
