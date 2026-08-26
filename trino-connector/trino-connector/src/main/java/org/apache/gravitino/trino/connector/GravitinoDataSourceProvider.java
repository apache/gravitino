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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;
import org.apache.gravitino.trino.connector.util.SpiVersionCompat;

/** This class provides a ConnectorPageSource for Trino read data from internal connector. */
// Trino 481 deprecates the split-based createPageSource for removal; it is still the only variant
// available on 435-481. Trino 482 removes it and is handled by a separate version-segment module.
@SuppressWarnings("removal")
public class GravitinoDataSourceProvider implements ConnectorPageSourceProvider {

  ConnectorPageSourceProvider internalPageSourceProvider;

  /**
   * Constructs a new GravitinoDataSourceProvider with the specified page source provider.
   *
   * @param pageSourceProvider the internal connector page source provider
   */
  public GravitinoDataSourceProvider(ConnectorPageSourceProvider pageSourceProvider) {
    this.internalPageSourceProvider = pageSourceProvider;
  }

  // Not annotated @Override: this split-based createPageSource is the SPI method up to Trino 481.
  // Trino 482 removed it; the 482-483 module supplies the credential/MemoryContext variant instead.
  // Kept so Trino 435-480 keep reading tables. The outbound call is dispatched reflectively so this
  // shared source still compiles against the Trino 482 SPI, where the method is never invoked.
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columns,
      DynamicFilter dynamicFilter) {
    return (ConnectorPageSource)
        SpiVersionCompat.invoke(
            internalPageSourceProvider,
            "createPageSource",
            new Class<?>[] {
              ConnectorTransactionHandle.class,
              ConnectorSession.class,
              ConnectorSplit.class,
              ConnectorTableHandle.class,
              List.class,
              DynamicFilter.class
            },
            GravitinoHandle.unWrap(transaction),
            session,
            GravitinoHandle.unWrap(split),
            GravitinoHandle.unWrap(table),
            GravitinoHandle.unWrap(columns),
            new GravitinoDynamicFilter(dynamicFilter));
  }
}
