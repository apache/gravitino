package org.apache.gravitino.catalog.starrocks.converter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStarRocksExceptionConverter {

  @Test
  public void testGetErrorCodeFromMessage() {
    String msg =
        "errCode = 2, detailMessage = Can't create database 'default_cluster:test_schema'; database exists";
    Assertions.assertEquals(
        StarRocksExceptionConverter.CODE_DATABASE_EXISTS,
        StarRocksExceptionConverter.getErrorCodeFromMessage(msg));

    msg =
        "errCode = 2, detailMessage = Can't drop database 'default_cluster:test_schema'; database doesn't exist";
    Assertions.assertEquals(
        StarRocksExceptionConverter.CODE_DATABASE_NOT_EXISTS,
        StarRocksExceptionConverter.getErrorCodeFromMessage(msg));

    msg = "errCode = 2, detailMessage = Unknown database 'default_cluster:no-exits'";
    Assertions.assertEquals(
        StarRocksExceptionConverter.CODE_UNKNOWN_DATABASE,
        StarRocksExceptionConverter.getErrorCodeFromMessage(msg));

    msg =
        "errCode = 2, detailMessage = Unknown table 'table_name' in default_cluster:database_name";
    Assertions.assertEquals(
        StarRocksExceptionConverter.CODE_NO_SUCH_TABLE,
        StarRocksExceptionConverter.getErrorCodeFromMessage(msg));
  }
}
