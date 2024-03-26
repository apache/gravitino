/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.system.storedprocdure;

import io.trino.spi.procedure.Procedure;

/** Gravitino System stored procedure interfaces */
public abstract class GravitinoStoredProcedure {

  /** Return the definition of the stored procedure */
  public abstract Procedure createStoredProcedure() throws Exception;
}
