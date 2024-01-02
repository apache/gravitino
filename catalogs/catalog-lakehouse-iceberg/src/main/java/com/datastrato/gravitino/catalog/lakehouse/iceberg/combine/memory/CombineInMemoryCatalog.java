/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.memory;

import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * CombineInMemoryCatalog is used to test, it shares the fileIO from another InMemory catalog, so
 * metadata create by other InMemory catalog could be accessed by current CombineInMemoryCatalog
 */
public class CombineInMemoryCatalog extends InMemoryCatalog {

  private FileIO fileIO;

  public CombineInMemoryCatalog(FileIO fileIO) {
    this.fileIO = fileIO;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    TableOperations operations =
        new CombineInMemoryTableOperations(fileIO, tableIdentifier, getTables());
    return operations;
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return super.defaultWarehouseLocation(tableIdentifier);
  }
}
