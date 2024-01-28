/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-it")
public class CatalogMysqlVersion5IT extends CatalogMysqlIT {
  public CatalogMysqlVersion5IT() {
    super();
    mysqlImageName = "mysql:5.7";
  }
}
