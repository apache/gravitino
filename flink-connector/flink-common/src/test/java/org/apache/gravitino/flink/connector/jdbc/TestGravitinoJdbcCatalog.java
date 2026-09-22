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

package org.apache.gravitino.flink.connector.jdbc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link GravitinoJdbcCatalog}. Concrete Flink version modules extend this and
 * supply their own {@link CatalogCompat}, since {@link GravitinoJdbcCatalog} is abstract.
 */
public abstract class TestGravitinoJdbcCatalog {

  protected abstract CatalogCompat catalogCompat();

  @Test
  void testGetFactoryDelegatesToCatalogCompat() {
    // GravitinoJdbcCatalog.getFactory() must return the version-appropriate JDBC dynamic table
    // factory via catalogCompat(), not a hardcoded factory -- the JDBC factory's package moved
    // (and, starting with Flink 2.x, the artifact was split by database) across Flink connector
    // releases, which is exactly what the CatalogCompat abstraction exists to paper over.
    GravitinoJdbcCatalog catalog =
        new TestableGravitinoJdbcCatalog(catalogCompat(), "test", Collections.emptyMap());

    Optional<Factory> factory = catalog.getFactory();

    Assertions.assertTrue(factory.isPresent());
    // jdbcDynamicTableFactory() constructs a new instance per call, so compare by type rather
    // than identity.
    Assertions.assertEquals(
        catalogCompat().jdbcDynamicTableFactory().getClass(), factory.get().getClass());
  }

  private static class TestableGravitinoJdbcCatalog extends GravitinoJdbcCatalog {
    private final CatalogCompat catalogCompat;

    TestableGravitinoJdbcCatalog(
        CatalogCompat catalogCompat, String catalogName, Map<String, String> options) {
      super(
          new MockCatalogContext(catalogName, options),
          "default",
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class));
      this.catalogCompat = catalogCompat;
    }

    @Override
    protected CatalogCompat catalogCompat() {
      return catalogCompat;
    }

    @Override
    protected AbstractCatalog createInnerCatalog(CatalogFactory.Context context) {
      throw new UnsupportedOperationException("not exercised by this test");
    }
  }

  private static class MockCatalogContext implements CatalogFactory.Context {
    private final String name;
    private final Map<String, String> options;

    MockCatalogContext(String name, Map<String, String> options) {
      this.name = name;
      this.options = options;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Map<String, String> getOptions() {
      return options;
    }

    @Override
    public ReadableConfig getConfiguration() {
      return Configuration.fromMap(options);
    }

    @Override
    public ClassLoader getClassLoader() {
      return Thread.currentThread().getContextClassLoader();
    }
  }

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

  @Test
  void testCredentialVendingOverridesOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "prop-user");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "prop-password");

    Catalog catalog =
        new CredentialCatalogStub(new JdbcCredential("vended-user", "vended-password"));
    GravitinoJdbcCatalog.applyJdbcCredential(catalog, options);

    Assertions.assertEquals("vended-user", options.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(
        "vended-password", options.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToOptionsWhenNoJdbcCredential() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "prop-user");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new CredentialCatalogStub(), options);

    Assertions.assertEquals("prop-user", options.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", options.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToOptionsWhenCatalogDoesNotSupportCredentials() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "prop-user");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new PlainCatalogStub(), options);

    Assertions.assertEquals("prop-user", options.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", options.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
  }
}
