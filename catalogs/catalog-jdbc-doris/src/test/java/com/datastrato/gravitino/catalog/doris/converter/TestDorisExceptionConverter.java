/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.converter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDorisExceptionConverter {
  @Test
  public void testGetErrorCodeFromMessage() {
    String msg =
        "errCode = 2, detailMessage = Can't create database 'default_cluster:test_schema'; database exists";
    Assertions.assertEquals(
        DorisExceptionConverter.CODE_DATABASE_EXISTS,
        DorisExceptionConverter.getErrorCodeFromMessage(msg));

    msg =
        "errCode = 2, detailMessage = Can't drop database 'default_cluster:test_schema'; database doesn't exist";
    Assertions.assertEquals(
        DorisExceptionConverter.CODE_DATABASE_NOT_EXISTS,
        DorisExceptionConverter.getErrorCodeFromMessage(msg));

    msg = "errCode = 2, detailMessage = Unknown database 'default_cluster:no-exits'";
    Assertions.assertEquals(
        DorisExceptionConverter.CODE_UNKNOWN_DATABASE,
        DorisExceptionConverter.getErrorCodeFromMessage(msg));

    msg =
        "errCode = 2, detailMessage = Unknown table 'table_name' in default_cluster:database_name";
    Assertions.assertEquals(
        DorisExceptionConverter.CODE_NO_SUCH_TABLE,
        DorisExceptionConverter.getErrorCodeFromMessage(msg));
  }
}
