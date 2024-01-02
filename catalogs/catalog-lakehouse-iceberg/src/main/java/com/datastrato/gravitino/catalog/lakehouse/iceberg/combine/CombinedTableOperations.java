/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinedTableOperations implements TableOperations {
  private static Logger LOG = LoggerFactory.getLogger(CombinedTableOperations.class);

  private TableOperations primaryTableOperations;
  private TableOperations secondaryTableOperations;

  public CombinedTableOperations(
      TableOperations primaryTableOperations, TableOperations secondaryTableOperations) {
    this.primaryTableOperations = primaryTableOperations;
    this.secondaryTableOperations = secondaryTableOperations;
  }

  @Override
  public TableMetadata current() {
    return primaryTableOperations.current();
  }

  @Override
  public TableMetadata refresh() {
    return primaryTableOperations.refresh();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    primaryTableOperations.commit(base, metadata);

    Utils.doSecondaryCatalogAction(
        () -> {
          // The Table metadata of Primary and secondary may not keep consistent,
          // So we use the table metadata from secondary not base table metadata from primary
          // catalog.
          // We must load metadata from Secondary before primary to grant consistent.
          // Please refer to
          // https://docs.google.com/document/d/1-OgmNdXmd_bfCruNcJCexZtrPms3xd-jCIM3064CYFY/edit
          // for more details
          TableMetadata secondaryLatest = secondaryTableOperations.refresh();
          TableMetadata primaryLatest = primaryTableOperations.refresh();
          // Overwrite writeNewMetadataIfRequired to share the metadata not writing a
          // new table metadata for secondary catalog
          secondaryTableOperations.commit(secondaryLatest, primaryLatest);
        });
  }

  @Override
  public FileIO io() {
    return primaryTableOperations.io();
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return primaryTableOperations.metadataFileLocation(fileName);
  }

  @Override
  public LocationProvider locationProvider() {
    return primaryTableOperations.locationProvider();
  }
}
