/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

public interface PartitionChange {

  String name();

  final class RenamePartition implements PartitionChange {
    private final String partitionName;
    private final String newName;

    private RenamePartition(String partitionName, String newName) {
      this.partitionName = partitionName;
      this.newName = newName;
    }

    @Override
    public String name() {
      return partitionName;
    }

    public String newName() {
      return newName;
    }
  }
}
