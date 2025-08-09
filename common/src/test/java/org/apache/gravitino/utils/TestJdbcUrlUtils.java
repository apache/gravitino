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
