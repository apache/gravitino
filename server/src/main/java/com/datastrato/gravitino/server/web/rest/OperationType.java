/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

public enum OperationType {
  LIST,
  CREATE,
  LOAD,
  ALTER,
  DROP;
}
