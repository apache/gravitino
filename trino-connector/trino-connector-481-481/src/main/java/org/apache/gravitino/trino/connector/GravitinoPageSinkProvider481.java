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
 * Trino 480 introduced {@code createPageSink}/{@code createMergeSink} variants that take an {@code
 * Optional<ConnectorTableCredentials>}; from Trino 481 the older variants without credentials are
 * deprecated and their default implementations throw, so the connector must delegate through the
 * credential-aware variants. Those types do not exist before Trino 480, so this version-specific
 * subclass lives in the version-segment module rather than the shared source.
 */
public class GravitinoPageSinkProvider481 extends GravitinoPageSinkProvider {

  /**
   * Constructs a new GravitinoPageSinkProvider481 with the specified page sink provider.
   *
   * @param pageSinkProvider the internal connector page sink provider
   */
  public GravitinoPageSinkProvider481(ConnectorPageSinkProvider pageSinkProvider) {
    super(pageSinkProvider);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    // GravitinoOutputTableHandle wraps a ConnectorInsertTableHandle internally,
    // so delegate to the insert-path createPageSink.
    ConnectorInsertTableHandle insertHandle =
        ((GravitinoOutputTableHandle) outputTableHandle).getInternalHandle();
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        insertHandle,
        tableCredentials,
        pageSinkId);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      Optional<ConnectorTableCredentials> tableCredentials,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
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
}
