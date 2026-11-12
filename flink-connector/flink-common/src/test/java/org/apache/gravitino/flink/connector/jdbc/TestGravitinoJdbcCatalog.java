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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for {@link GravitinoJdbcCatalog}. */
public abstract class TestGravitinoJdbcCatalog {

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

  @Test
  void testInnerCatalogUsesConnectionDatabase() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_URL, "jdbc:postgresql://127.0.0.1:5432/");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE, "public");
    options.put(JdbcPropertiesConstants.GRAVITINO_CONNECTION_DATABASE, "gravitino");
    TestableGravitinoJdbcCatalog catalog =
        new TestableGravitinoJdbcCatalog(catalogContext(options), "public");

    Assertions.assertThrows(UnsupportedOperationException.class, catalog::open);

    Map<String, String> innerOptions = catalog.getInnerCatalogOptions();
    Assertions.assertEquals(
        "jdbc:postgresql://127.0.0.1:5432",
        innerOptions.get(JdbcPropertiesConstants.FLINK_JDBC_URL));
    Assertions.assertEquals(
        "gravitino", innerOptions.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
    Assertions.assertFalse(
        innerOptions.containsKey(JdbcPropertiesConstants.GRAVITINO_CONNECTION_DATABASE));
    Assertions.assertEquals("public", catalog.getDefaultDatabase());
  }

  @Test
  void testNativeCatalogConnectsToConnectionDatabase() throws IOException {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    String hostUrl = "127.0.0.1:" + port;
    Map<String, String> expectedUrls =
        ImmutableMap.of(
            "jdbc:postgresql://" + hostUrl + "/", "jdbc:postgresql://" + hostUrl + "/gravitino",
            "jdbc:postgresql://" + hostUrl, "jdbc:postgresql://" + hostUrl + "/gravitino",
            "jdbc:mysql://" + hostUrl + "/", "jdbc:mysql://" + hostUrl + "/gravitino");
    for (Map.Entry<String, String> entry : expectedUrls.entrySet()) {
      Map<String, String> options = new HashMap<>();
      options.put(JdbcPropertiesConstants.FLINK_JDBC_URL, entry.getKey());
      options.put(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE, "public");
      options.put(JdbcPropertiesConstants.GRAVITINO_CONNECTION_DATABASE, "gravitino");
      options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "user");
      options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "password");
      GravitinoJdbcCatalog catalog = createNativeCatalog(catalogContext(options), "public");

      // Nothing listens on the port, so the native catalog fails on its first connection and
      // reports the URL. The MySQL catalog connects when it is created, PostgreSQL in open().
      RuntimeException exception = Assertions.assertThrows(RuntimeException.class, catalog::open);
      Assertions.assertTrue(
          exception instanceof ValidationException || exception instanceof CatalogException,
          exception.toString());
      String message = exception.getMessage();
      Assertions.assertTrue(
          message.contains(entry.getValue() + " via") || message.endsWith(entry.getValue() + "."),
          message);
    }
  }

  /**
   * Creates a JDBC catalog using the implementation for the Flink version under test.
   *
   * @param context the catalog factory context
   * @param defaultDatabase the default Flink database
   * @return the version-specific JDBC catalog
   */
  protected abstract GravitinoJdbcCatalog createNativeCatalog(
      CatalogFactory.Context context, String defaultDatabase);

  private static CatalogFactory.Context catalogContext(Map<String, String> options) {
    return new FactoryUtil.DefaultCatalogContext(
        "test", options, new Configuration(), TestGravitinoJdbcCatalog.class.getClassLoader());
  }

  private static class TestableGravitinoJdbcCatalog extends GravitinoJdbcCatalog {
    private Map<String, String> innerCatalogOptions;

    TestableGravitinoJdbcCatalog(CatalogFactory.Context context, String defaultDatabase) {
      super(
          context,
          defaultDatabase,
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class));
    }

    @Override
    protected AbstractCatalog createInnerCatalog(CatalogFactory.Context context) {
      innerCatalogOptions = context.getOptions();
      throw new UnsupportedOperationException("The inner catalog must not be created");
    }

    @Override
    protected Catalog catalog() {
      return new PlainCatalogStub();
    }

    Map<String, String> getInnerCatalogOptions() {
      return innerCatalogOptions;
    }
  }
}
