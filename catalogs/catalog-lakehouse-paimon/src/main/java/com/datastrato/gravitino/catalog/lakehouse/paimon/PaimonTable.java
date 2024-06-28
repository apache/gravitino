/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.connector.BaseTable;
import com.datastrato.gravitino.connector.TableOperations;
import lombok.Getter;
import lombok.ToString;
import org.apache.paimon.table.Table;

/** Implementation of {@link Table} that represents a Paimon Table entity in the Paimon table. */
@ToString
@Getter
public class PaimonTable extends BaseTable {

  private PaimonTable() {}

  @Override
  protected TableOperations newOps() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
}
