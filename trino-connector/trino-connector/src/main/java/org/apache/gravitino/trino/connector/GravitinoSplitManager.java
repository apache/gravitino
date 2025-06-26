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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

/** This class delegates the retrieval of split data sources to optimize query performance. */
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
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    ConnectorSplitSource splits =
        internalSplitManager.getSplits(
            GravitinoHandle.unWrap(transaction),
            session,
            GravitinoHandle.unWrap(connectorTableHandle),
            new GravitinoDynamicFilter(dynamicFilter),
            constraint);
    return new GravitinoSplitSource(splits);
  }
}
