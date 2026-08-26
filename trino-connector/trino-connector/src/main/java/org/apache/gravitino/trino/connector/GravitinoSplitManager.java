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
import io.trino.spi.connector.DynamicFilter;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.trino.connector.util.SpiVersionCompat;

/**
 * This class delegates the retrieval of split data sources to optimize query performance.
 *
 * <p>Trino 482 changed {@code ConnectorSplitManager.getSplits} to take a {@code Set<ColumnHandle>}
 * of dynamic-filter columns instead of a {@code DynamicFilter}. Both variants are declared here so
 * the shared source compiles against every supported Trino SPI; Trino 435-481 dispatch to the
 * {@code DynamicFilter} variant and Trino 482+ dispatch to the {@code Set<ColumnHandle>} variant.
 * The outbound calls are made reflectively because each internal overload only exists on its own
 * range of Trino versions.
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

  // Not annotated @Override: this DynamicFilter variant is the SPI method up to Trino 481 but was
  // replaced by the Set<ColumnHandle> variant in Trino 482. Kept for Trino 435-481; dead on 482+.
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    ConnectorSplitSource splits =
        (ConnectorSplitSource)
            SpiVersionCompat.invoke(
                internalSplitManager,
                "getSplits",
                new Class<?>[] {
                  ConnectorTransactionHandle.class,
                  ConnectorSession.class,
                  ConnectorTableHandle.class,
                  DynamicFilter.class,
                  Constraint.class
                },
                GravitinoHandle.unWrap(transaction),
                session,
                GravitinoHandle.unWrap(connectorTableHandle),
                new GravitinoDynamicFilter(dynamicFilter),
                new GravitinoConstraint(constraint));
    return createSplitSource(splits);
  }

  // Not annotated @Override: this Set<ColumnHandle> variant is the SPI method from Trino 482
  // onward;
  // on Trino 435-481 it is an inert extra method (the DynamicFilter variant above is used instead).
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      Set<ColumnHandle> dynamicFilterColumns,
      Constraint constraint) {
    Set<ColumnHandle> unwrappedColumns =
        dynamicFilterColumns.stream().map(GravitinoHandle::unWrap).collect(Collectors.toSet());
    ConnectorSplitSource splits =
        (ConnectorSplitSource)
            SpiVersionCompat.invoke(
                internalSplitManager,
                "getSplits",
                new Class<?>[] {
                  ConnectorTransactionHandle.class,
                  ConnectorSession.class,
                  ConnectorTableHandle.class,
                  Set.class,
                  Constraint.class
                },
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
