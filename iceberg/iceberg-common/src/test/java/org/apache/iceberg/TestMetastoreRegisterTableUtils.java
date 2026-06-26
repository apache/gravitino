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
package org.apache.iceberg;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetastoreRegisterTableUtils {

  @Test
  void testRegisterTableWithoutOverwriteDelegatesToCatalog() {
    BaseMetastoreCatalog catalog = mock(BaseMetastoreCatalog.class);
    MetastoreRegisterTableUtils.MetadataLocationOverwrite metadataLocationOverwrite =
        mock(MetastoreRegisterTableUtils.MetadataLocationOverwrite.class);
    TableIdentifier identifier = TableIdentifier.of("ns", "t");
    String metadataLocation = "s3://bucket/ns/t/metadata/v1.metadata.json";
    Table table = mock(Table.class);

    when(catalog.registerTable(identifier, metadataLocation)).thenReturn(table);

    Table result =
        MetastoreRegisterTableUtils.registerTable(
            catalog, identifier, metadataLocation, false, metadataLocationOverwrite);

    Assertions.assertSame(table, result);
    verify(catalog).registerTable(identifier, metadataLocation);
    verifyNoInteractions(metadataLocationOverwrite);
  }

  @Test
  void testRegisterTableWithOverwriteRejectsEmptyMetadataLocation() {
    BaseMetastoreCatalog catalog = mock(BaseMetastoreCatalog.class);
    MetastoreRegisterTableUtils.MetadataLocationOverwrite metadataLocationOverwrite =
        mock(MetastoreRegisterTableUtils.MetadataLocationOverwrite.class);
    TableIdentifier identifier = TableIdentifier.of("ns", "t");

    when(catalog.isValidIdentifier(identifier)).thenReturn(true);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MetastoreRegisterTableUtils.registerTable(
                catalog, identifier, "", true, metadataLocationOverwrite));
  }
}
