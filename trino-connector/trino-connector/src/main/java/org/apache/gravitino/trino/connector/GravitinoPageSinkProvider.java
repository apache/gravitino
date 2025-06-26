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
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.commons.lang3.NotImplementedException;

/** This class provides a ConnectorPageSink for Trino to write data to internal connector. */
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
    throw new NotImplementedException();
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle,
      ConnectorPageSinkId pageSinkId) {
    return pageSinkProvider.createPageSink(
        GravitinoHandle.unWrap(transactionHandle),
        session,
        GravitinoHandle.unWrap(insertTableHandle),
        pageSinkId);
  }
}
