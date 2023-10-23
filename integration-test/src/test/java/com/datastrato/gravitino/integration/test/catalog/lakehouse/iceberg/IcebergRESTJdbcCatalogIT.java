/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTJdbcCatalogIT extends IcebergRESTServiceIT {
  public IcebergRESTJdbcCatalogIT() {
    catalogType = IcebergCatalogBackend.JDBC;
  }
}
