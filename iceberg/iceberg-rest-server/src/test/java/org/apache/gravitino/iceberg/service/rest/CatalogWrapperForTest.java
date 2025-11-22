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
package org.apache.gravitino.iceberg.service.rest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

// Used to override registerTable
@SuppressWarnings("deprecation")
public class CatalogWrapperForTest extends CatalogWrapperForREST {
  public static final String GENERATE_PLAN_TASKS_DATA_PROP = "test.generate-plan-data";

  public CatalogWrapperForTest(String catalogName, IcebergConfig icebergConfig) {
    super(catalogName, icebergConfig);
  }

  @Override
  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.createTable(namespace, request, requestCredential);
    if (shouldGeneratePlanTasksData(request)) {
      appendSampleData(namespace, request.name());
    }
    return loadTableResponse;
  }

  @Override
  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    if (request.name().contains("fail")) {
      throw new AlreadyExistsException("Already exits exception for test");
    }

    Schema mockSchema = new Schema(NestedField.of(1, false, "foo_string", StringType.get()));
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            mockSchema, PartitionSpec.unpartitioned(), "/mock", ImmutableMap.of());
    LoadTableResponse loadTableResponse =
        LoadTableResponse.builder()
            .withTableMetadata(tableMetadata)
            .addAllConfig(ImmutableMap.of())
            .build();
    return loadTableResponse;
  }

  private boolean shouldGeneratePlanTasksData(CreateTableRequest request) {
    if (request.properties() == null) {
      return false;
    }
    return Boolean.parseBoolean(
        request.properties().getOrDefault(GENERATE_PLAN_TASKS_DATA_PROP, Boolean.FALSE.toString()));
  }

  private void appendSampleData(Namespace namespace, String tableName) {
    try {
      Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
      Path tempFile = Files.createTempFile("plan-scan", ".parquet");
      DataFile dataFile =
          DataFiles.builder(table.spec())
              .withPath(tempFile.toUri().toString())
              .withFormat(FileFormat.PARQUET)
              .withRecordCount(1)
              .withFileSizeInBytes(0L)
              .build();
      table.newFastAppend().appendFile(dataFile).commit();
      super.loadTable(TableIdentifier.of(namespace, tableName));
    } catch (IOException e) {
      throw new RuntimeException("Failed to append sample data for test table " + tableName, e);
    }
  }
}
