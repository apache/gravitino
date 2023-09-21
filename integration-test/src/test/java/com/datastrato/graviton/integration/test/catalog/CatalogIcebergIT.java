/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test.catalog;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("graviton-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogIcebergIT extends CatalogHiveIT {

  @Override
  protected String getProvider() {
    return "iceberg";
  }
}
