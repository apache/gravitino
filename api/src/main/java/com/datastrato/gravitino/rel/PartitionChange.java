/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

public interface PartitionChange {

  /** @return The name of the partition to be changed. */
  String name();
}
