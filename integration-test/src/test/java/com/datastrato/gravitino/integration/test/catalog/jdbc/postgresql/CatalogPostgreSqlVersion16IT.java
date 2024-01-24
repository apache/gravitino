/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

public class CatalogPostgreSqlVersion16IT extends CatalogPostgreSqlIT {
  public CatalogPostgreSqlVersion16IT() {
    postgreImageName = "postgres:16";
  }
}
