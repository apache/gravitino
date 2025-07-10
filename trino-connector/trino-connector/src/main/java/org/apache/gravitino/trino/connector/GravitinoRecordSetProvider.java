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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import java.util.List;

/** This class provides a RecordSet for Trino read data from internal connector. */
public class GravitinoRecordSetProvider implements ConnectorRecordSetProvider {

  ConnectorRecordSetProvider internalRecordSetProvider;

  /**
   * Constructs a new GravitinoRecordSetProvider with the specified record set provider.
   *
   * @param recordSetProvider the internal connector record set provider
   */
  public GravitinoRecordSetProvider(ConnectorRecordSetProvider recordSetProvider) {
    this.internalRecordSetProvider = recordSetProvider;
  }

  @Override
  public RecordSet getRecordSet(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<? extends ColumnHandle> columns) {
    return internalRecordSetProvider.getRecordSet(
        GravitinoHandle.unWrap(transaction),
        session,
        GravitinoHandle.unWrap(split),
        GravitinoHandle.unWrap(table),
        GravitinoHandle.unWrap(columns));
  }
}
