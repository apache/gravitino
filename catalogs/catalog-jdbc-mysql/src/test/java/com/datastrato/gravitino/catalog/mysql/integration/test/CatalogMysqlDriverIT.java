/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.mysql.integration.test;

import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-it")
public class CatalogMysqlDriverIT extends CatalogMysqlIT {
  public CatalogMysqlDriverIT() {
    super();
    mysqlDriverDownloadUrl =
        "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.11/mysql-connector-java-8.0.11.jar";
  }
}
