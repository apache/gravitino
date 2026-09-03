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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import java.util.List;
import java.util.Optional;

/**
 * Trino 482 reworked {@code createPageSource}: the split-based (non-credential) variant was removed
 * and the primary read entry point now takes an {@code Optional<ConnectorTableCredentials>} and a
 * {@code MemoryContext}. Both {@code ConnectorTableCredentials} (added in Trino 480) and {@code
 * MemoryContext} (added in Trino 482) are unavailable on the older Trino SPIs the shared source
 * compiles against, so this Trino 482+ variant lives in the version-segment module.
 */
public class GravitinoDataSourceProvider482 extends GravitinoDataSourceProvider {

  /**
   * Constructs a new GravitinoDataSourceProvider482 with the specified page source provider.
   *
   * @param pageSourceProvider the internal connector page source provider
   */
  public GravitinoDataSourceProvider482(ConnectorPageSourceProvider pageSourceProvider) {
    super(pageSourceProvider);
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      Optional<ConnectorTableCredentials> tableCredentials,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter,
      MemoryContext memoryContext) {
    return internalPageSourceProvider.createPageSource(
        GravitinoHandle.unWrap(transaction),
        session,
        GravitinoHandle.unWrap(split),
        GravitinoHandle.unWrap(table),
        tableCredentials,
        GravitinoHandle.unWrap(columns),
        new GravitinoDynamicFilter(dynamicFilter),
        memoryContext);
  }
}
