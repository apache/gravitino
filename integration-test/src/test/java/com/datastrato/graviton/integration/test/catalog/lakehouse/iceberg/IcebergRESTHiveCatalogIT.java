/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.graviton.catalog.lakehouse.iceberg.utils.IcebergCatalogUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

// Hive&Jdbc catalog must be tested with graviton-docker-it env,
// so create a seperate class not using junit `parameterized test` to auto generate catalog type
@Tag("graviton-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTHiveCatalogIT extends IcebergRESTServiceIT {
  public IcebergRESTHiveCatalogIT() {
    catalogType = IcebergCatalogUtil.CatalogType.HIVE;
  }
}
