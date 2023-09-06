/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.meta.rel.BaseSortOrder;

public class HiveSortOrder extends BaseSortOrder {

  public static class Builder extends BaseSortOrder.BaseSortOrderBuilder<Builder, HiveSortOrder> {
    @Override
    public HiveSortOrder build() {
      HiveSortOrder sortOrder = new HiveSortOrder();
      sortOrder.transform = transform;
      sortOrder.direction = direction;
      sortOrder.nullOrder = nullOrder;
      return sortOrder;
    }
  }
}
