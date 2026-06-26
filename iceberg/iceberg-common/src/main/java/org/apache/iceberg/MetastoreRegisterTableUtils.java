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

import com.google.common.base.Preconditions;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Shared helpers for {@link org.apache.iceberg.catalog.Catalog#registerTable(TableIdentifier,
 * String, boolean)} on {@link BaseMetastoreCatalog} implementations where Iceberg's default
 * implementation does not support {@code overwrite=true}.
 */
public class MetastoreRegisterTableUtils {

  /** Atomically overwrites the metastore metadata location during register overwrite. */
  public interface MetadataLocationOverwrite {
    /**
     * @param identifier table identifier
     * @param expectedMetadataLocation expected current metadata file location
     * @param newMetadataLocation metadata file location to register
     */
    void overwrite(
        TableIdentifier identifier, String expectedMetadataLocation, String newMetadataLocation);
  }

  /**
   * Registers a table from an existing metadata file, optionally overwriting an existing
   * registration by replacing the metastore metadata location.
   *
   * @param catalog metastore catalog that performs the registration
   * @param identifier table identifier to register
   * @param metadataFileLocation location of the metadata file to register
   * @param overwrite whether to overwrite an existing table registration
   * @param metadataLocationOverwrite backend-specific atomic metadata location overwrite
   * @return the registered table
   */
  public static Table registerTable(
      BaseMetastoreCatalog catalog,
      TableIdentifier identifier,
      String metadataFileLocation,
      boolean overwrite,
      MetadataLocationOverwrite metadataLocationOverwrite) {
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

    TableMetadataParser.read(metastoreOps.io().newInputFile(metadataFileLocation));

    String expectedMetadataLocation = base.metadataFileLocation();
    metadataLocationOverwrite.overwrite(identifier, expectedMetadataLocation, metadataFileLocation);
    metastoreOps.refresh();

    return new BaseTable(
        ops,
        BaseMetastoreCatalog.fullTableName(catalog.name(), identifier),
        catalog.metricsReporter());
  }

  private MetastoreRegisterTableUtils() {}
}
