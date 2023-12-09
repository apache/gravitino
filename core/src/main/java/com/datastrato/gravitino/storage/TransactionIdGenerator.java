/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import java.io.Closeable;

/** Generator id number as transaction, which will be used as MVCC version. */
public interface TransactionIdGenerator extends Closeable {

  /**
   * Next transaction id. The transaction id is a monotonically increasing number, which is used as
   * MVCC version.
   *
   * @return next transaction id
   */
  long nextId();

  /** Start the generator. */
  void start();
}
