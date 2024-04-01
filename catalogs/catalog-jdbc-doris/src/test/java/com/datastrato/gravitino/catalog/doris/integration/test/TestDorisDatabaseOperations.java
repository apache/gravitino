/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

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
    String databaseName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    String databaseComment = "test_comment";

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          DATABASE_OPERATIONS.create(databaseName, databaseComment, properties);
        });
  }
}
