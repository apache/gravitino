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

package org.apache.gravitino.utils;

import java.util.Map;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcUrlUtils {

  @Test
  public void whenMalformedUrlGiven_ShouldThrowGravitinoRuntimeException() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver", "malformed%ZZurl", Map.of("test", "test")));
    Assertions.assertEquals("Unable to decode JDBC URL", gre.getMessage());
  }

  @Test
  public void testValidateJdbcConfigWhenDriverClassNameIsNull() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                null, "jdbc:mysql://localhost:0000/test", Map.of("test", "test")));
  }

  @Test
  public void testValidateJdbcConfigForMySQL() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver", "jdbc:mysql://localhost:0000/test", Map.of("test", "test")));
  }

  @Test
  public void whenUnsafeParameterGivenForMySQL_ShouldThrowGravitinoRuntimeException() {

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test?allowloadlocalinfile=test",
                    Map.of("test", "test")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'allowloadlocalinfile' detected in JDBC URL", gre.getMessage());
  }

  @Test
  public void whenConfigPropertiesMapContainsUnsafeParam_ShouldThrowGravitinoRuntimeException() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Map.of("maxAllowedPacket", "maxAllowedPacket")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'maxAllowedPacket' detected in JDBC URL", gre.getMessage());
  }

  @Test
  public void testValidateJdbcConfigForMariaDB() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver", "jdbc:mariadb://localhost:0000/test", Map.of("test", "test")));
  }

  @Test
  public void whenUnsafeParameterGivenForMariaDB_ShouldThrowGravitintoRuntimeException() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mariaDB://localhost:0000/test?allowloadlocalinfile=test",
                    Map.of("test", "test")));
    Assertions.assertEquals(
        "Unsafe MariaDB parameter 'allowloadlocalinfile' detected in JDBC URL", gre.getMessage());
  }

  @Test
  public void testValidateJdbcConfigForPostgreSQL() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver", "jdbc:postgresql://localhost:0000/test", Map.of("test", "test")));
  }

  @Test
  public void whenUnsafeParameterGivenForPostgreSQL_ShouldThrowGravitinoRuntimeException() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:postgresql://localhost:0000/test?socketFactory=test",
                    Map.of("test", "test")));
    Assertions.assertEquals(
        "Unsafe PostgreSQL parameter 'socketFactory' detected in JDBC URL", gre.getMessage());
  }
}
