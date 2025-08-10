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

import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

// Used to override registerTable
@SuppressWarnings("deprecation")
public class CatalogWrapperForTest extends CatalogWrapperForREST {
  public CatalogWrapperForTest(String catalogName, IcebergConfig icebergConfig) {
    super(catalogName, icebergConfig);
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
}
