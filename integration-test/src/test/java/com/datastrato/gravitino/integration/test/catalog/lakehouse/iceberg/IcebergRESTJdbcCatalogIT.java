/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTJdbcCatalogIT extends IcebergRESTServiceIT {
  public IcebergRESTJdbcCatalogIT() {
    catalogType = IcebergCatalogBackend.JDBC;
  }

  @Test
  void test() {
    System.out.println("heeloss");
  }
}
