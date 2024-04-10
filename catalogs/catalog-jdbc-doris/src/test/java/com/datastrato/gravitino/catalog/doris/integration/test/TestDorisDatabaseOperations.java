/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.utils.RandomNameUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestDorisDatabaseOperations extends TestDorisAbstractIT {

  @Test
  public void testBaseOperationDatabase() {
    String databaseName = RandomNameUtils.genRandomName("it_db");
    String comment = "comment";

    Map<String, String> properties = new HashMap<>();
    properties.put("property1", "value1");

    testBaseOperation(databaseName, properties, comment);

    // recreate database, get exception.
    Assertions.assertThrowsExactly(
        SchemaAlreadyExistsException.class,
        () -> DATABASE_OPERATIONS.create(databaseName, "", properties));

    testDropDatabase(databaseName);
  }
}
