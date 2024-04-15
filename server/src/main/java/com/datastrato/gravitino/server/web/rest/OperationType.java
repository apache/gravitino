/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

public enum OperationType {
  LIST,
  CREATE,
  LOAD,
  ALTER,
  DROP,
  /** This is a special operation type that is used to get a partition from a table. */
  GET,
  ADD,
  REMOVE
}
