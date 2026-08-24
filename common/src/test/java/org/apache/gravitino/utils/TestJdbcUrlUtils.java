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

import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

public class TestJdbcUrlUtils {

  @Test
  public void whenMalformedUrlGiven_ShouldThrowGravitinoRuntimeException() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver", "malformed%ZZurl", Collections.singletonMap("test", "test")));
    Assertions.assertEquals("Unable to decode JDBC URL", gre.getMessage());
  }

  @Test
  public void testValidateJdbcConfigWhenDriverClassNameIsNull() {
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                null,
                "jdbc:mysql://localhost:0000/test",
                Collections.singletonMap("test", "test")));
  }

  @Test
  public void testValidateJdbcConfigForMySQL() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:mysql://localhost:0000/test",
                Collections.singletonMap("test", "test")));
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
                    Collections.singletonMap("test", "test")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'allowloadlocalinfile' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConfigPropertiesMapContainsUnsafeParam_ShouldThrowGravitinoRuntimeException() {
    // The unsafe param appears only as the config KEY (value is unrelated), so this exercises the
    // key-based detection rather than incidentally matching on the value.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap("maxAllowedPacket", "1024")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'maxAllowedPacket' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void testValidateJdbcConfigForMariaDB() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:mariadb://localhost:0000/test",
                Collections.singletonMap("test", "test")));
  }

  @Test
  public void whenUnsafeParameterGivenForMariaDB_ShouldThrowGravitinoRuntimeException() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mariaDB://localhost:0000/test?allowloadlocalinfile=test",
                    Collections.singletonMap("test", "test")));
    Assertions.assertEquals(
        "Unsafe MariaDB parameter 'allowloadlocalinfile' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void testValidateJdbcConfigForPostgreSQL() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:postgresql://localhost:0000/test",
                Collections.singletonMap("test", "test")));
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
                    Collections.singletonMap("test", "test")));
    Assertions.assertEquals(
        "Unsafe PostgreSQL parameter 'socketFactory' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  void testValidateNullArguments() {
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                null,
                "jdbc:postgresql://localhost:0000/test",
                Collections.singletonMap("test", "test")));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver", null, Collections.singletonMap("test", "test")));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> JdbcUrlUtils.validateJdbcConfig(null, null, null));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> JdbcUrlUtils.validateJdbcConfig("", "jdbc:postgresql://localhost:0000/test", null));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> JdbcUrlUtils.validateJdbcConfig("testDriver", "", null));
  }

  @Test
  public void whenConfigIsNullAndUrlIsSafe_ShouldNotThrow() {
    // A recognized DB URL with a null config map must reach the parameter check without NPE and
    // pass (guards the null-config short-circuit in collectConfigParameterNames).
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver", "jdbc:mysql://localhost:0000/test", null));
  }

  @Test
  public void whenConnectionPropertiesContainsUnsafeParam_ShouldThrowForMySQL() {
    // DBCP2's BasicDataSourceFactory recognizes a "connectionProperties" key whose value is a
    // ';'-delimited list of driver connection properties (e.g. "k1=v1;k2=v2"). These are passed
    // straight to the JDBC driver, so an unsafe MySQL parameter smuggled here bypasses a check
    // that only inspects the URL string and the raw config values.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap("connectionProperties", "autoDeserialize=true")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesContainsMultipleUnsafeParams_ShouldThrowForMySQL() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap(
                        "connectionProperties",
                        "useCompression=true;queryInterceptors=com.example.Evil")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'queryInterceptors' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesContainsUnsafeParam_ShouldThrowForPostgreSQL() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:postgresql://localhost:0000/test",
                    Collections.singletonMap(
                        "connectionProperties", "socketFactory=com.example.Evil")));
    Assertions.assertEquals(
        "Unsafe PostgreSQL parameter 'socketFactory' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenUnsafeParamGivenAsConfigKey_ShouldThrowForMySQL() {
    // Defense in depth: an unsafe parameter supplied directly as a config key (not embedded in the
    // URL) must also be rejected.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap("autoDeserialize", "true")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenSafeConnectionPropertiesGiven_ShouldNotThrow() {
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:mysql://localhost:0000/test",
                Collections.singletonMap(
                    "connectionProperties", "useCompression=true;connectTimeout=1000")));
  }

  @Test
  public void whenConnectionPropertyNameContainsUnsafeSubstring_ShouldNotThrow() {
    // Adversarial negative control: a safe name that merely CONTAINS an unsafe param as a
    // substring must not be rejected — proves whole-name (not substring) matching on the config
    // side.
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:mysql://localhost:0000/test",
                Collections.singletonMap("connectionProperties", "autoDeserializeHelper=true")));
  }

  @Test
  public void whenUnsafeParamAppearsOnlyAsConfigValue_ShouldNotThrow() {
    // The check inspects parameter NAMES (config keys + connectionProperties names), not values —
    // only names reach the driver as connection parameters. An unsafe token appearing solely as a
    // value under a benign key must not be rejected. Guards against reintroducing the removed
    // value-scanning heuristic, which would reject legitimate catalogs.
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:mysql://localhost:0000/test",
                Collections.singletonMap("jdbc-password", "autoDeserialize")));
  }

  @Test
  public void whenConnectionPropertyValueEqualsUnsafeName_ShouldNotThrow() {
    // Same principle inside connectionProperties: 'autoDeserialize' as the VALUE of a benign
    // property name is harmless (the driver receives name 'foo'), so it must not be rejected.
    Assertions.assertDoesNotThrow(
        () ->
            JdbcUrlUtils.validateJdbcConfig(
                "testDriver",
                "jdbc:mysql://localhost:0000/test",
                Collections.singletonMap("connectionProperties", "foo=autoDeserialize")));
  }

  @Test
  public void whenConnectionPropertiesSmugglesUnsafeParamViaNewline_ShouldThrowForMySQL() {
    // DBCP2 parses the connectionProperties value by replacing ';' with '\n' and calling
    // Properties.load, so an embedded newline starts a new property. A parser that split only on
    // ';' would miss the second name; parsing exactly as DBCP2 does catches it.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap(
                        "connectionProperties", "foo=bar\nautoDeserialize=true")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesSmugglesUnsafeParamViaUnicodeEscape_ShouldThrowForMySQL() {
    // "\\u0061utoDeserialize" is decoded to "autoDeserialize" by Properties.load. A naive
    // substring parser would see the literal "\\u0061..." and miss it.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap(
                        "connectionProperties", "\\u0061utoDeserialize=true")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesKeyHasDifferentCase_ShouldThrowForMySQL() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap("ConnectionProperties", "autoDeserialize=true")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesHasWhitespaceAroundPairs_ShouldThrowForMySQL() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap(
                        "connectionProperties", "useCompression=true; autoDeserialize = true")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesContainsUnsafeParam_ShouldThrowForMariaDB() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mariadb://localhost:0000/test",
                    Collections.singletonMap("connectionProperties", "autoDeserialize=true")));
    Assertions.assertEquals(
        "Unsafe MariaDB parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenUnsafeParamGivenAsConfigKey_ShouldThrowForPostgreSQL() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:postgresql://localhost:0000/test",
                    Collections.singletonMap("socketFactory", "com.example.Evil")));
    Assertions.assertEquals(
        "Unsafe PostgreSQL parameter 'socketFactory' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesHasTrailingSeparators_ShouldStillThrow() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap("connectionProperties", "autoDeserialize=true;;")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenConnectionPropertiesValueIsMalformed_ShouldThrowUnableToParse() {
    // A "\\uZZZZ" escape makes Properties.load throw IllegalArgumentException; the validator
    // rejects the un-inspectable value rather than letting it reach the driver (DBCP2 parses it
    // the same way and would also fail).
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test",
                    Collections.singletonMap("connectionProperties", "\\uZZZZ=true")));
    Assertions.assertEquals("Unable to parse JDBC connectionProperties", gre.getMessage());
    // The parse cause is chained for server-side diagnostics; guard against a "simplification"
    // that drops it.
    Assertions.assertInstanceOf(IllegalArgumentException.class, gre.getCause());
  }

  @Test
  public void whenUnsafeParamGivenAsConfigKey_ShouldThrowForMariaDB() {
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mariadb://localhost:0000/test",
                    Collections.singletonMap("autoDeserialize", "true")));
    Assertions.assertEquals(
        "Unsafe MariaDB parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenPercentEscapeDecodesToUpperCaseParam_ShouldThrowForMySQL() {
    // "%41" decodes to uppercase 'A', so after recursiveDecode the URL contains
    // "AllowLoadLocalInfile". The check must re-lower-case the decoded URL (not rely on the
    // pre-decode lower-casing) to still match the unsafe parameter.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test?%41llowLoadLocalInfile=true",
                    Collections.singletonMap("test", "test")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'allowloadlocalinfile' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  public void whenParamIsDoubleUrlEncoded_ShouldThrowForMySQL() {
    // "%2561" is a double-encoding of 'a': one decode pass yields "%61", a second yields 'a'. The
    // check must decode recursively (not a single pass) to collapse nested encoding before
    // matching, otherwise the unsafe parameter slips through.
    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                JdbcUrlUtils.validateJdbcConfig(
                    "testDriver",
                    "jdbc:mysql://localhost:0000/test?%2561llowLoadLocalInfile=true",
                    Collections.singletonMap("test", "test")));
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'allowloadlocalinfile' detected in JDBC configuration",
        gre.getMessage());
  }

  @Test
  @ResourceLock(value = Resources.LOCALE, mode = ResourceAccessMode.READ_WRITE)
  public void whenDefaultLocaleIsTurkish_ShouldStillMatchParamWithI() {
    // The attacker-supplied form carries a capital 'I' ('statementInterceptors'). Under the
    // Turkish locale a default-locale toLowerCase() folds 'I' to a dotless 'ı', while Locale.ROOT
    // folds it to an ASCII dotted 'i' to match the unsafe-param list entry. Using the capital-'I'
    // input at each site means a Locale.ROOT -> toLowerCase() regression at ANY of the four
    // input-lowercasing sites (URL pre-decode, URL post-decode, config key, connectionProperties
    // name) diverges from the ROOT-folded param and is caught here — a default-locale CI would not
    // otherwise catch it.
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr"));

      // URL path — literal capital 'I' is folded at the pre-decode site.
      Assertions.assertEquals(
          "Unsafe MySQL parameter 'statementInterceptors' detected in JDBC configuration",
          Assertions.assertThrows(
                  GravitinoRuntimeException.class,
                  () ->
                      JdbcUrlUtils.validateJdbcConfig(
                          "testDriver",
                          "jdbc:mysql://localhost:0000/test?statementInterceptors=com.example.Evil",
                          Collections.singletonMap("test", "test")))
              .getMessage());

      // URL path — '%49' decodes to 'I' only AFTER recursiveDecode, so this exercises the
      // post-decode re-lowercasing site specifically.
      Assertions.assertEquals(
          "Unsafe MySQL parameter 'statementInterceptors' detected in JDBC configuration",
          Assertions.assertThrows(
                  GravitinoRuntimeException.class,
                  () ->
                      JdbcUrlUtils.validateJdbcConfig(
                          "testDriver",
                          "jdbc:mysql://localhost:0000/test?statement%49nterceptors=com.example.Evil",
                          Collections.singletonMap("test", "test")))
              .getMessage());

      // Config-key path.
      Assertions.assertEquals(
          "Unsafe MySQL parameter 'statementInterceptors' detected in JDBC configuration",
          Assertions.assertThrows(
                  GravitinoRuntimeException.class,
                  () ->
                      JdbcUrlUtils.validateJdbcConfig(
                          "testDriver",
                          "jdbc:mysql://localhost:0000/test",
                          Collections.singletonMap("statementInterceptors", "com.example.Evil")))
              .getMessage());

      // connectionProperties-name path.
      Assertions.assertEquals(
          "Unsafe MySQL parameter 'statementInterceptors' detected in JDBC configuration",
          Assertions.assertThrows(
                  GravitinoRuntimeException.class,
                  () ->
                      JdbcUrlUtils.validateJdbcConfig(
                          "testDriver",
                          "jdbc:mysql://localhost:0000/test",
                          Collections.singletonMap(
                              "connectionProperties", "statementInterceptors=com.example.Evil")))
              .getMessage());
    } finally {
      Locale.setDefault(previous);
    }
  }
}
