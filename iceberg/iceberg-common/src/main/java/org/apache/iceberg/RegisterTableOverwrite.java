/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg;

import com.google.common.base.Preconditions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveRegisterTablePointerSwap;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryRegisterTablePointerSwap;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcRegisterTablePointerSwap;

/**
 * Shared implementation of {@link Catalog#registerTable(TableIdentifier, String, boolean)} for
 * {@link BaseMetastoreCatalog} backends that only provide the two-argument register path.
 */
public final class RegisterTableOverwrite {

  private RegisterTableOverwrite() {}

  /**
   * Registers a table from an existing metadata file, optionally overwriting an existing
   * registration.
   *
   * @param catalog metastore catalog that owns table operations for the identifier
   * @param identifier table identifier to register
   * @param metadataFileLocation location of the metadata file to register
   * @param overwrite whether to overwrite an existing table registration
   * @return the registered table
   */
  public static Table registerTable(
      BaseMetastoreCatalog catalog,
      TableIdentifier identifier,
      String metadataFileLocation,
      boolean overwrite) {
    if (!overwrite) {
      return catalog.registerTable(identifier, metadataFileLocation);
    }

    Preconditions.checkArgument(
        identifier != null && catalog.isValidIdentifier(identifier),
        "Invalid identifier: %s",
        identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a table");

    if (!catalog.tableExists(identifier)) {
      return catalog.registerTable(identifier, metadataFileLocation);
    }

    TableOperations ops = catalog.newTableOps(identifier);
    Preconditions.checkArgument(
        ops instanceof BaseMetastoreTableOperations,
        "Cannot register table %s: table operations are not metastore-backed",
        identifier);
    BaseMetastoreTableOperations metastoreOps = (BaseMetastoreTableOperations) ops;

    metastoreOps.refresh();
    TableMetadata base = metastoreOps.current();
    Preconditions.checkArgument(
        base != null, "Cannot overwrite table %s: current metadata is missing", identifier);

    InputFile metadataFile = metastoreOps.io().newInputFile(metadataFileLocation);
    TableMetadataParser.read(metadataFile);

    String oldMetadataLocation = base.metadataFileLocation();
    swapPointer(catalog, identifier, oldMetadataLocation, metadataFileLocation);
    metastoreOps.refreshFromMetadataLocation(metadataFileLocation);

    return new BaseTable(
        ops,
        BaseMetastoreCatalog.fullTableName(catalog.name(), identifier),
        catalog.metricsReporter());
  }

  private static void swapPointer(
      BaseMetastoreCatalog catalog,
      TableIdentifier identifier,
      String oldMetadataLocation,
      String newMetadataLocation) {
    if (catalog instanceof JdbcCatalog) {
      JdbcRegisterTablePointerSwap.swap(
          (JdbcCatalog) catalog, identifier, oldMetadataLocation, newMetadataLocation);
    } else if (catalog instanceof InMemoryCatalog) {
      InMemoryRegisterTablePointerSwap.swap(
          (InMemoryCatalog) catalog, identifier, oldMetadataLocation, newMetadataLocation);
    } else if (catalog instanceof HiveCatalog) {
      HiveRegisterTablePointerSwap.swap(
          (HiveCatalog) catalog, identifier, oldMetadataLocation, newMetadataLocation);
    } else {
      throw new UnsupportedOperationException(
          catalog.getClass().getName() + " does not support registerTable with overwrite");
    }
  }
}
