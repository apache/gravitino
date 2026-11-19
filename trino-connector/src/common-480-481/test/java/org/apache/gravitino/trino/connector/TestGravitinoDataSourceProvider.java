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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for the Trino 480/481 {@link GravitinoDataSourceProvider}. */
@SuppressWarnings("removal")
class TestGravitinoDataSourceProvider {

  private final ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);
  private final ConnectorSplit split = mock(ConnectorSplit.class);
  private final ConnectorTableHandle table = mock(ConnectorTableHandle.class);
  private final ColumnHandle column = mock(ColumnHandle.class);
  private final ConnectorSession session = mock(ConnectorSession.class);
  private final DynamicFilter dynamicFilter = mock(DynamicFilter.class);
  private final ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
  private final ConnectorPageSourceProvider internalProvider =
      mock(ConnectorPageSourceProvider.class);
  private final GravitinoDataSourceProvider provider =
      new GravitinoDataSourceProvider(internalProvider);

  @Test
  void testCredentialVariantDelegatesWithUnwrappedHandlesAndCredentials() {
    ConnectorTableCredentials internalCredentials = mock(ConnectorTableCredentials.class);
    Optional<ConnectorTableCredentials> credentials = Optional.of(internalCredentials);
    when(internalProvider.createPageSource(
            eq(transaction),
            eq(session),
            eq(split),
            eq(table),
            eq(credentials),
            eq(List.of(column)),
            any()))
        .thenReturn(pageSource);

    ConnectorPageSource result =
        provider.createPageSource(
            new GravitinoTransactionHandle(transaction),
            session,
            new GravitinoSplit(split) {},
            new GravitinoTableHandle("s", "t", table),
            Optional.of(new GravitinoTableCredentials(internalCredentials)),
            List.of(new GravitinoColumnHandle("c1", column)),
            dynamicFilter);

    assertThat(result).isSameAs(pageSource);
    ArgumentCaptor<DynamicFilter> filter = ArgumentCaptor.forClass(DynamicFilter.class);
    verify(internalProvider)
        .createPageSource(
            eq(transaction),
            eq(session),
            eq(split),
            eq(table),
            eq(credentials),
            eq(List.of(column)),
            filter.capture());
    assertThat(filter.getValue()).isInstanceOf(GravitinoDynamicFilter.class);
    verifyNoMoreInteractions(internalProvider);
  }

  @Test
  void testLegacyVariantDelegatesWithUnwrappedHandles() {
    when(internalProvider.createPageSource(
            eq(transaction), eq(session), eq(split), eq(table), eq(List.of(column)), any()))
        .thenReturn(pageSource);

    ConnectorPageSource result =
        provider.createPageSource(
            new GravitinoTransactionHandle(transaction),
            session,
            new GravitinoSplit(split) {},
            new GravitinoTableHandle("s", "t", table),
            List.of(new GravitinoColumnHandle("c1", column)),
            dynamicFilter);

    assertThat(result).isSameAs(pageSource);
    verify(internalProvider)
        .createPageSource(
            eq(transaction),
            eq(session),
            eq(split),
            eq(table),
            eq(List.of(column)),
            any(GravitinoDynamicFilter.class));
    verifyNoMoreInteractions(internalProvider);
  }
}
