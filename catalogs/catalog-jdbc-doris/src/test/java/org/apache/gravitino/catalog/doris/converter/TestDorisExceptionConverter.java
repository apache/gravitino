/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.doris.converter;

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

    msg = "errCode = 2, detailMessage = Syntax error in line 3: unexpected token: AUTO";
    Assertions.assertEquals(
        DorisExceptionConverter.CODE_BUCKETS_AUTO_NOT_SUPPORTED,
        DorisExceptionConverter.getErrorCodeFromMessage(msg));

    msg =
        "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'AUTO' at line 1";
    Assertions.assertEquals(
        DorisExceptionConverter.CODE_BUCKETS_AUTO_NOT_SUPPORTED,
        DorisExceptionConverter.getErrorCodeFromMessage(msg));
  }
}
