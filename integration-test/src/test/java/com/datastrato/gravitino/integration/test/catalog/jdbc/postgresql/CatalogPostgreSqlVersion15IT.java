/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-it")
public class CatalogPostgreSqlVersion15IT extends CatalogPostgreSqlIT {
  public CatalogPostgreSqlVersion15IT() {
    postgreImageName = "postgres:15";
  }
}
