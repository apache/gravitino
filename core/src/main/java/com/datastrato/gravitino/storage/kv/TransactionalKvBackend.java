/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import java.io.IOException;

public interface TransactionalKvBackend extends KvBackend {

  /** Begin the transaction. */
  void begin();

  /**
   * Commit the transaction.
   *
   * @throws IOException if the commit operation fails
   */
  void commit() throws IOException;

  /**
   * Rollback the transaction if something goes wrong.
   *
   * @throws IOException if the rollback operation fails
   */
  void rollback() throws IOException;

  /** Close the current transaction. */
  void closeTransaction();

  /**
   * Check whether the backend is in transaction in the current thread.
   *
   * @return true if the backend is in transaction in the current thread
   */
  boolean inTransaction();
}
