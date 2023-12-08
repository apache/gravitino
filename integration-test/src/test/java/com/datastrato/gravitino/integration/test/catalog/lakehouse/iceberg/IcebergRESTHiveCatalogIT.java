/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

// Hive&Jdbc catalog must be tested with gravitino-docker-it env,
// so we should create a separate class instead using junit `parameterized test`
// to auto-generate catalog type
@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTHiveCatalogIT extends IcebergRESTServiceIT {
  public IcebergRESTHiveCatalogIT() {
    catalogType = IcebergCatalogBackend.HIVE;
  }
}
