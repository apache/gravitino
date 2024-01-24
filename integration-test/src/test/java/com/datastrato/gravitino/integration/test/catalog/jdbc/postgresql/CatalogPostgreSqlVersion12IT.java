/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

public class CatalogPostgreSqlVersion12IT extends CatalogPostgreSqlIT {
  public CatalogPostgreSqlVersion12IT() {
    postgreImageName = "postgres:12";
  }
}
