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

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.statistics.ComputedStatistics;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;

public class GravitinoMetadata477 extends GravitinoMetadata {

  private final CatalogConnectorMetadata catalogConnectorMetadata;
  private final CatalogConnectorMetadataAdapter metadataAdapter;

  public GravitinoMetadata477(
      CatalogConnectorMetadata catalogConnectorMetadata,
      CatalogConnectorMetadataAdapter metadataAdapter,
      io.trino.spi.connector.ConnectorMetadata internalMetadata) {
    super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    this.catalogConnectorMetadata = catalogConnectorMetadata;
    this.metadataAdapter = metadataAdapter;
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(
      ConnectorSession session,
      ConnectorInsertTableHandle insertHandle,
      List<ConnectorTableHandle> sourceTableHandles,
      Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    return internalMetadata.finishInsert(
        session,
        GravitinoHandle.unWrap(insertHandle),
        sourceTableHandles.stream().map(GravitinoHandle::unWrap).collect(Collectors.toList()),
        fragments,
        computedStatistics);
  }

  @Override
  public ConnectorMergeTableHandle beginMerge(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
      RetryMode retryMode) {
    return internalMetadata.beginMerge(
        session, GravitinoHandle.unWrap(tableHandle), updateCaseColumns, retryMode);
  }

  @Override
  public void addColumn(
      ConnectorSession session,
      ConnectorTableHandle tableHandle,
      ColumnMetadata column,
      ColumnPosition position) {
    GravitinoColumn gravitinoColumn = metadataAdapter.createColumn(column);
    catalogConnectorMetadata.addColumn(getTableName(tableHandle), gravitinoColumn);
  }
}
