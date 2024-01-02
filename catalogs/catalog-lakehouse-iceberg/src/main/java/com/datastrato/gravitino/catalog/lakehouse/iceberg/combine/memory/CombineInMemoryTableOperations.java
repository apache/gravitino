/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.memory;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.Utils;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.memory.InMemoryCatalog.InMemoryTableOperations;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * CombineInMemoryTableOperations is used to override writeNewMetadata, to reuse metadata location
 * from TableMetaData to commit. The latest TableMetadata of primary catalog is refreshed by
 * CombineCatalog to grant the existence of metadata location in TableMetaData.
 */
public class CombineInMemoryTableOperations extends InMemoryTableOperations {
  public CombineInMemoryTableOperations(
      FileIO fileIO,
      TableIdentifier tableIdentifier,
      ConcurrentMap<TableIdentifier, String> tables) {
    super(fileIO, tableIdentifier, tables);
  }

  @Override
  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    return Utils.resueMetadataLocation(metadata);
  }
}
