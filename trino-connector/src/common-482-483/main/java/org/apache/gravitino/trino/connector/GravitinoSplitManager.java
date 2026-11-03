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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class delegates the retrieval of split data sources to optimize query performance.
 *
 * <p>Trino 482 replaced {@code ConnectorSplitManager.getSplits}'s {@code DynamicFilter} parameter
 * with a {@code Set<ColumnHandle>} of dynamic-filter columns, so only that variant is implemented.
 */
public class GravitinoSplitManager implements ConnectorSplitManager {

  private final ConnectorSplitManager internalSplitManager;

  /**
   * Constructs a new GravitinoSplitManager with the specified split manager.
   *
   * @param internalSplitManager the internal connector split manager
   */
  public GravitinoSplitManager(ConnectorSplitManager internalSplitManager) {
    this.internalSplitManager = internalSplitManager;
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      Set<ColumnHandle> dynamicFilterColumns,
      Constraint constraint) {
    Set<ColumnHandle> unwrappedColumns =
        dynamicFilterColumns.stream().map(GravitinoHandle::unWrap).collect(Collectors.toSet());
    ConnectorSplitSource splits =
        internalSplitManager.getSplits(
            GravitinoHandle.unWrap(transaction),
            session,
            GravitinoHandle.unWrap(connectorTableHandle),
            unwrappedColumns,
            new GravitinoConstraint(constraint));
    return createSplitSource(splits);
  }

  protected ConnectorSplitSource createSplitSource(ConnectorSplitSource splits) {
    throw new TrinoException(NOT_SUPPORTED, "Should be overridden in subclass");
  }
}
