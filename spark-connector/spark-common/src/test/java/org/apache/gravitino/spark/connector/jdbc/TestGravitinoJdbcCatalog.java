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

package org.apache.gravitino.spark.connector.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
<<<<<<< HEAD
=======
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
>>>>>>> 2043e62ea ([#14848] improvement(spark-connector): Let JDBC catalog subclasses opt in to preferTimestampNTZ (#14849))
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Unit tests for {@link GravitinoJdbcCatalog#applyJdbcCredential}. */
public class TestGravitinoJdbcCatalog {

  /**
   * Minimal Catalog stub — returns nothing, used to verify the instanceof check for
   * SupportsCredentials.
   */
  private static class PlainCatalogStub implements Catalog {
    @Override
    public String name() {
      return "stub";
    }

    @Override
    public Type type() {
      return Type.RELATIONAL;
    }

    @Override
    public String provider() {
      return "jdbc-mysql";
    }

    @Override
    public String comment() {
      return null;
    }

    @Override
    public Map<String, String> properties() {
      return Collections.emptyMap();
    }

    @Override
    public Audit auditInfo() {
      return null;
    }
  }

  /** Catalog that also implements SupportsCredentials — simulates the upgraded server. */
  private static class CredentialCatalogStub extends PlainCatalogStub
      implements SupportsCredentials {

    private final Credential[] credentials;

    CredentialCatalogStub(Credential... credentials) {
      this.credentials = credentials;
    }

    @Override
    public Credential[] getCredentials() {
      return credentials;
    }

    @Override
    public SupportsCredentials supportsCredentials() {
      return this;
    }
  }

<<<<<<< HEAD
=======
  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc-postgresql",
        "jdbc-mysql",
        "jdbc-doris",
        "jdbc-starrocks",
        "jdbc-oceanbase",
        "jdbc-oracle"
      })
  void testTimestampMappingIsUnchangedByDefault(String provider) throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(credentials.getCredentials())
          .thenReturn(new Credential[] {new JdbcCredential("user", "password")});
      GravitinoJdbcCatalog catalog = catalog(provider, credentials);
      JDBCOptions options = initialize(catalog, Map.of(), Map.of());
      Assertions.assertFalse(options.preferTimestampNTZ());
      Assertions.assertEquals(
          DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withoutTimeZone()));
      Assertions.assertEquals(
          DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withTimeZone()));
      Assertions.assertEquals("password", options.asConnectionProperties().getProperty("password"));
      verify(credentials).getCredentials();
    }
  }

  @ParameterizedTest
  @MethodSource("nonPostgreSqlNtzOptions")
  void testNonPostgreSqlNtzOptionsAreIgnored(
      String provider, Map<String, String> catalogOptions, Map<String, String> properties)
      throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(credentials.getCredentials()).thenReturn(new Credential[0]);
      GravitinoJdbcCatalog catalog = catalog(provider, credentials);
      JDBCOptions options = initialize(catalog, catalogOptions, properties);
      Assertions.assertFalse(options.preferTimestampNTZ());
      Assertions.assertEquals(
          DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withoutTimeZone()));
      Assertions.assertEquals(
          1L,
          options.asProperties().stringPropertyNames().stream()
              .filter("preferTimestampNTZ"::equalsIgnoreCase)
              .count());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc-postgresql",
        "jdbc-mysql",
        "jdbc-doris",
        "jdbc-starrocks",
        "jdbc-oceanbase",
        "jdbc-oracle"
      })
  void testSessionTimestampTypeDoesNotEnableNtz(String provider) throws Exception {
    SQLConf conf = SQLConf.get();
    String previous = conf.getConfString("spark.sql.timestampType", null);
    try {
      conf.setConfString("spark.sql.timestampType", "TIMESTAMP_NTZ");
      try (MockedStatic<GravitinoCatalogManager> global =
          mockStatic(GravitinoCatalogManager.class)) {
        global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
        SupportsCredentials credentials = mock(SupportsCredentials.class);
        when(credentials.getCredentials()).thenReturn(new Credential[0]);
        GravitinoJdbcCatalog catalog = catalog(provider, credentials);
        Assertions.assertFalse(initialize(catalog, Map.of(), Map.of()).preferTimestampNTZ());
        Assertions.assertEquals(
            DataTypes.TimestampType,
            catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withoutTimeZone()));
      }
    } finally {
      if (previous == null) {
        conf.unsetConf("spark.sql.timestampType");
      } else {
        conf.setConfString("spark.sql.timestampType", previous);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
    "preferTimestampNTZ, true, '', true",
    "prefertimestampntz, true, '', true",
    "PREFERTIMESTAMPNTZ, false, '', false",
    "preferTimestampNTZ, '', true, true",
    "prefertimestampntz, '', true, true",
    "PreferTimestampNtz, '', false, false",
    "preferTimestampNTZ, false, true, false",
    "prefertimestampntz, true, false, true",
    "PreferTimestampNtz, true, false, true",
    "PREFERTIMESTAMPNTZ, false, true, false",
    "prefertimestampntz, '', true, true"
  })
  void testPostgreSqlNtzOptionDrivesReaderAndConverter(
      String key, String option, String property, boolean expected) throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(credentials.getCredentials()).thenReturn(new Credential[0]);
      GravitinoJdbcCatalog catalog = catalog("jdbc-postgresql", credentials);
      // A Spark catalog option wins over a Gravitino catalog property with the "spark.bypass."
      // prefix.
      JDBCOptions options =
          initialize(
              catalog,
              option.isEmpty() ? Map.of() : Map.of(key, option),
              property.isEmpty() ? Map.of() : Map.of("spark.bypass." + key, property));
      Assertions.assertEquals(expected, options.preferTimestampNTZ());
      Assertions.assertEquals(
          expected ? DataTypes.TimestampNTZType : DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withoutTimeZone()));
    }
  }

  /** A catalog for another provider that opts in to preferTimestampNTZ. */
  private static class OptInCatalog extends GravitinoJdbcCatalog {
    @Override
    protected boolean supportsPreferTimestampNtz() {
      return true;
    }

    @Override
    protected SparkTypeConverter getSparkTypeConverter() {
      return new SparkJdbcTypeConverter(useTimestampNtz());
    }
  }

  @ParameterizedTest
  @CsvSource({"true, true", "false, false", "'', false"})
  void testSubclassCanOptInToNtzOption(String option, boolean expected) throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(credentials.getCredentials()).thenReturn(new Credential[0]);
      Catalog source = mock(Catalog.class);
      when(source.provider()).thenReturn("jdbc-mysql");
      when(source.supportsCredentials()).thenReturn(credentials);
      OptInCatalog catalog = new OptInCatalog();
      FieldUtils.writeField(catalog, "gravitinoCatalogClient", source, true);

      JDBCOptions options =
          initialize(
              catalog,
              option.isEmpty() ? Map.of() : Map.of("preferTimestampNTZ", option),
              Map.of());
      Assertions.assertEquals(expected, options.preferTimestampNTZ());
      Assertions.assertEquals(expected, catalog.useTimestampNtz());
      Assertions.assertEquals(
          expected ? DataTypes.TimestampNTZType : DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withoutTimeZone()));
    }
  }

  @Test
  void testOptedInCatalogStillRejectsInvalidNtzValue() throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(credentials.getCredentials()).thenReturn(new Credential[0]);
      Catalog source = mock(Catalog.class);
      when(source.provider()).thenReturn("jdbc-mysql");
      when(source.supportsCredentials()).thenReturn(credentials);
      OptInCatalog catalog = new OptInCatalog();
      FieldUtils.writeField(catalog, "gravitinoCatalogClient", source, true);

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> initialize(catalog, Map.of("preferTimestampNTZ", "yes"), Map.of()));
    }
  }

>>>>>>> 2043e62ea ([#14848] improvement(spark-connector): Let JDBC catalog subclasses opt in to preferTimestampNTZ (#14849))
  @Test
  void testSpark33RejectsUnsupportedTimestampNtzBeforeOpeningReader() throws Exception {
    try (MockedStatic<GravitinoCatalogManager> managers =
        mockStatic(GravitinoCatalogManager.class)) {
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      Catalog source = mock(Catalog.class);
      when(source.provider()).thenReturn("jdbc-postgresql");
      when(source.supportsCredentials()).thenReturn(new CredentialCatalogStub());
      FieldUtils.writeField(catalog, "gravitinoCatalogClient", source, true);
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  catalog.createAndInitSparkCatalog(
                      "pg33",
                      new CaseInsensitiveStringMap(Map.of("preferTimestampNTZ", "true")),
                      Map.of(
                          "jdbc-url",
                          "jdbc:postgresql://localhost/db",
                          "jdbc-driver",
                          "org.postgresql.Driver")));
      Assertions.assertTrue(error.getMessage().contains("pg33"));
      Assertions.assertTrue(error.getMessage().contains("Spark 3.5"));
      Assertions.assertTrue(error.getMessage().contains("use false"));
    }
  }

  @Test
  void testCredentialVendingOverridesProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcPropertiesConstants.SPARK_JDBC_USER, "prop-user");
    props.put(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, "prop-password");

    Catalog catalog =
        new CredentialCatalogStub(new JdbcCredential("vended-user", "vended-password"));
    GravitinoJdbcCatalog.applyJdbcCredential(catalog, props);

    Assertions.assertEquals("vended-user", props.get(JdbcPropertiesConstants.SPARK_JDBC_USER));
    Assertions.assertEquals(
        "vended-password", props.get(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToPropertiesWhenNoJdbcCredential() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcPropertiesConstants.SPARK_JDBC_USER, "prop-user");
    props.put(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new CredentialCatalogStub(), props);

    Assertions.assertEquals("prop-user", props.get(JdbcPropertiesConstants.SPARK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", props.get(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToPropertiesWhenCatalogDoesNotSupportCredentials() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcPropertiesConstants.SPARK_JDBC_USER, "prop-user");
    props.put(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new PlainCatalogStub(), props);

    Assertions.assertEquals("prop-user", props.get(JdbcPropertiesConstants.SPARK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", props.get(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD));
  }
}
