/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.extension.DummyCredentialProvider;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogWrapperForREST {

  private static final AtomicBoolean CONSTRUCTION_IN_PROGRESS = new AtomicBoolean(false);

  @Test
  void testCheckPropertiesForCompatibility() {
    ImmutableMap<String, String> deprecatedMap = ImmutableMap.of("deprecated", "new");
    ImmutableMap<String, String> propertiesWithDeprecatedKey = ImmutableMap.of("deprecated", "v");
    Map<String, String> newProperties =
        CatalogWrapperForREST.checkForCompatibility(propertiesWithDeprecatedKey, deprecatedMap);
    Assertions.assertEquals(newProperties, ImmutableMap.of("new", "v"));

    ImmutableMap<String, String> propertiesWithoutDeprecatedKey = ImmutableMap.of("k", "v");
    newProperties =
        CatalogWrapperForREST.checkForCompatibility(propertiesWithoutDeprecatedKey, deprecatedMap);
    Assertions.assertEquals(newProperties, ImmutableMap.of("k", "v"));

    ImmutableMap<String, String> propertiesWithBothKey =
        ImmutableMap.of("deprecated", "v", "new", "v");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.checkForCompatibility(propertiesWithBothKey, deprecatedMap));
  }

  @Test
  void testIsLocalOrHdfsLocation() {
    Assertions.assertTrue(CatalogWrapperForREST.isLocalOrHdfsLocation("/tmp/warehouse"));
    Assertions.assertTrue(CatalogWrapperForREST.isLocalOrHdfsLocation("file:///tmp/warehouse"));
    Assertions.assertTrue(
        CatalogWrapperForREST.isLocalOrHdfsLocation("hdfs://localhost:9000/warehouse"));

    Assertions.assertFalse(CatalogWrapperForREST.isLocalOrHdfsLocation("s3://bucket/warehouse"));
    Assertions.assertFalse(
        CatalogWrapperForREST.isLocalOrHdfsLocation("abfs://container@account/warehouse"));
    Assertions.assertFalse(CatalogWrapperForREST.isLocalOrHdfsLocation(""));
    Assertions.assertFalse(CatalogWrapperForREST.isLocalOrHdfsLocation("   "));
  }

  @Test
  void testRESTTableCredentials() throws Exception {
    TableIdentifier table = TableIdentifier.of(Namespace.of("db"), "tbl");
    String expectedPath = "/v1/upstream/namespaces/db/tables/tbl/credentials";
    String upstreamJson =
        "{\"storage-credentials\":[{\"prefix\":\"s3://upstream/db/tbl/\",\"config\":{"
            + "\"s3.access-key-id\":\"upstream-key\","
            + "\"s3.secret-access-key\":\"upstream-secret\","
            + "\"s3.session-token\":\"upstream-token\"}}]}";

    AtomicReference<String> requestPath = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestPath.set(exchange.getRequestURI().getPath());
          byte[] body = upstreamJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      RESTCatalog restCatalog = mock(RESTCatalog.class);
      when(restCatalog.name()).thenReturn("upstream");
      when(restCatalog.properties())
          .thenReturn(
              ImmutableMap.of(
                  CatalogProperties.URI,
                  uri,
                  AuthProperties.AUTH_TYPE,
                  AuthProperties.AUTH_TYPE_NONE,
                  "prefix",
                  "upstream"));

      IcebergConfig config =
          new IcebergConfig(
              ImmutableMap.of(
                  IcebergConstants.CATALOG_BACKEND,
                  "memory",
                  IcebergConstants.WAREHOUSE,
                  "/tmp/warehouse"));
      CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("local", config, restCatalog);

      LoadCredentialsResponse response =
          wrapper.getTableCredentials(table, CredentialPrivilege.READ);

      Assertions.assertEquals(expectedPath, requestPath.get());
      Assertions.assertEquals(1, response.credentials().size());
      Credential credential = response.credentials().get(0);
      Assertions.assertEquals("s3://upstream/db/tbl/", credential.prefix());
      Assertions.assertEquals("upstream-key", credential.config().get("s3.access-key-id"));
      Assertions.assertEquals("upstream-secret", credential.config().get("s3.secret-access-key"));
      Assertions.assertEquals("upstream-token", credential.config().get("s3.session-token"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testRESTTableCredentialsOnFailure() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      RESTCatalog restCatalog = mock(RESTCatalog.class);
      when(restCatalog.name()).thenReturn("upstream");
      when(restCatalog.properties())
          .thenReturn(
              ImmutableMap.of(
                  CatalogProperties.URI,
                  uri,
                  AuthProperties.AUTH_TYPE,
                  AuthProperties.AUTH_TYPE_NONE,
                  "prefix",
                  "upstream"));

      IcebergConfig config =
          new IcebergConfig(
              ImmutableMap.of(
                  IcebergConstants.CATALOG_BACKEND,
                  "memory",
                  IcebergConstants.WAREHOUSE,
                  "/tmp/warehouse"));
      CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("local", config, restCatalog);

      Assertions.assertThrows(
          ServiceFailureException.class,
          () ->
              wrapper.getTableCredentials(
                  TableIdentifier.of(Namespace.of("db"), "tbl"), CredentialPrivilege.READ));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testRESTTableCredentialsOnForbidden() throws Exception {
    String errorJson =
        "{\"error\":{\"message\":\"Forbidden\",\"type\":\"ForbiddenException\","
            + "\"code\":403,\"stack\":[]}}";
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body = errorJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(403, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      RESTCatalog restCatalog = mock(RESTCatalog.class);
      when(restCatalog.name()).thenReturn("upstream");
      when(restCatalog.properties())
          .thenReturn(
              ImmutableMap.of(
                  CatalogProperties.URI,
                  uri,
                  AuthProperties.AUTH_TYPE,
                  AuthProperties.AUTH_TYPE_NONE,
                  "prefix",
                  "upstream"));

      IcebergConfig config =
          new IcebergConfig(
              ImmutableMap.of(
                  IcebergConstants.CATALOG_BACKEND,
                  "memory",
                  IcebergConstants.WAREHOUSE,
                  "/tmp/warehouse"));
      CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("local", config, restCatalog);

      Assertions.assertThrows(
          ForbiddenException.class,
          () ->
              wrapper.getTableCredentials(
                  TableIdentifier.of(Namespace.of("db"), "tbl"), CredentialPrivilege.READ));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testRESTTableCredentialsOnUnauthorized() throws Exception {
    String errorJson =
        "{\"error\":{\"message\":\"Not authorized\",\"type\":\"NotAuthorizedException\","
            + "\"code\":401,\"stack\":[]}}";
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body = errorJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(401, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      RESTCatalog restCatalog = mock(RESTCatalog.class);
      when(restCatalog.name()).thenReturn("upstream");
      when(restCatalog.properties())
          .thenReturn(
              ImmutableMap.of(
                  CatalogProperties.URI,
                  uri,
                  AuthProperties.AUTH_TYPE,
                  AuthProperties.AUTH_TYPE_NONE,
                  "prefix",
                  "upstream"));

      IcebergConfig config =
          new IcebergConfig(
              ImmutableMap.of(
                  IcebergConstants.CATALOG_BACKEND,
                  "memory",
                  IcebergConstants.WAREHOUSE,
                  "/tmp/warehouse"));
      CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("local", config, restCatalog);

      Assertions.assertThrows(
          NotAuthorizedException.class,
          () ->
              wrapper.getTableCredentials(
                  TableIdentifier.of(Namespace.of("db"), "tbl"), CredentialPrivilege.READ));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testRESTTableCredentialsNoSuchTable() throws Exception {
    String errorJson =
        "{\"error\":{\"message\":\"Table not found\",\"type\":\"NoSuchTableException\","
            + "\"code\":404,\"stack\":[]}}";
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body = errorJson.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(404, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      RESTCatalog restCatalog = mock(RESTCatalog.class);
      when(restCatalog.name()).thenReturn("upstream");
      when(restCatalog.properties())
          .thenReturn(
              ImmutableMap.of(
                  CatalogProperties.URI,
                  uri,
                  AuthProperties.AUTH_TYPE,
                  AuthProperties.AUTH_TYPE_NONE,
                  "prefix",
                  "upstream"));

      IcebergConfig config =
          new IcebergConfig(
              ImmutableMap.of(
                  IcebergConstants.CATALOG_BACKEND,
                  "memory",
                  IcebergConstants.WAREHOUSE,
                  "/tmp/warehouse"));
      CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("local", config, restCatalog);

      Assertions.assertThrows(
          NoSuchTableException.class,
          () ->
              wrapper.getTableCredentials(
                  TableIdentifier.of(Namespace.of("db"), "missing"), CredentialPrivilege.READ));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testLocalTableCredentials() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse",
                CredentialConstants.CREDENTIAL_PROVIDERS,
                DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE));

    CatalogWrapperForREST wrapper = new CatalogWrapperForREST("local-catalog", config);
    Namespace namespace = Namespace.of("db");
    Catalog catalog = wrapper.getCatalog();
    ((SupportsNamespaces) catalog).createNamespace(namespace);
    TableIdentifier table = TableIdentifier.of(namespace, "tbl");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    catalog.createTable(
        table,
        schema,
        PartitionSpec.unpartitioned(),
        "s3://bucket/wh/db/tbl",
        Collections.emptyMap());

    LoadCredentialsResponse response = wrapper.getTableCredentials(table, CredentialPrivilege.READ);

    Assertions.assertEquals(1, response.credentials().size());
    Assertions.assertEquals("s3://bucket/wh/db/tbl", response.credentials().get(0).prefix());
  }

  @Test
  void testValidateCredentialLocation() {
    Assertions.assertDoesNotThrow(
        () -> CatalogWrapperForREST.validateCredentialLocation("/tmp/warehouse"));
    Assertions.assertDoesNotThrow(
        () -> CatalogWrapperForREST.validateCredentialLocation("file:///tmp/warehouse"));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> CatalogWrapperForREST.validateCredentialLocation(""));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.validateCredentialLocation("   "));
  }

  @Test
  void testLoadTableRefreshEndpoint() {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "tbl");
    RESTCatalog catalog = mock(RESTCatalog.class);
    BaseTable baseTable = mock(BaseTable.class);
    TableOperations ops = mock(TableOperations.class);
    FileIO fileIO = mock(FileIO.class);
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "s3://bucket/db/tbl",
            Collections.emptyMap());

    when(catalog.loadTable(ident)).thenReturn(baseTable);
    when(baseTable.operations()).thenReturn(ops);
    when(ops.current()).thenReturn(metadata);
    when(baseTable.io()).thenReturn(fileIO);
    when(fileIO.properties())
        .thenReturn(
            ImmutableMap.of(
                "s3.session-token",
                "token",
                "s3.session-token-expires-at-ms",
                "123",
                "client.refresh-credentials-endpoint",
                "v1/upstream/namespaces/db/tables/tbl/credentials"));

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("irc1", config, catalog);

    LoadTableResponse response = wrapper.loadTable(ident, false, CredentialPrivilege.READ);

    Assertions.assertEquals(
        "v1/irc1/namespaces/db/tables/tbl/credentials",
        response.config().get("client.refresh-credentials-endpoint"));
    Assertions.assertEquals("token", response.config().get("s3.session-token"));
  }

  @Test
  void testLoadTableStorageCreds() {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "tbl");
    RESTCatalog catalog = mock(RESTCatalog.class);
    BaseTable baseTable = mock(BaseTable.class);
    TableOperations ops = mock(TableOperations.class);
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    SupportsStorageCredentials storageCredentialsFileIO = (SupportsStorageCredentials) fileIO;
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "s3://bucket/db/tbl",
            Collections.emptyMap());

    StorageCredential upstreamCredential =
        StorageCredential.create(
            "s3://bucket/db/tbl/",
            ImmutableMap.of(
                "s3.access-key-id",
                "upstream-key",
                "s3.secret-access-key",
                "upstream-secret",
                "s3.session-token",
                "upstream-token",
                "s3.session-token-expires-at-ms",
                "123",
                "client.refresh-credentials-endpoint",
                "v1/upstream/namespaces/db/tables/tbl/credentials"));

    when(catalog.loadTable(ident)).thenReturn(baseTable);
    when(baseTable.operations()).thenReturn(ops);
    when(ops.current()).thenReturn(metadata);
    when(baseTable.io()).thenReturn(fileIO);
    when(fileIO.properties()).thenReturn(Collections.emptyMap());
    when(storageCredentialsFileIO.credentials()).thenReturn(List.of(upstreamCredential));

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("irc1", config, catalog);

    LoadTableResponse response = wrapper.loadTable(ident, false, CredentialPrivilege.READ);

    Assertions.assertEquals(1, response.credentials().size());
    Credential credential = response.credentials().get(0);
    Assertions.assertEquals("s3://bucket/db/tbl/", credential.prefix());
    Assertions.assertEquals("upstream-token", credential.config().get("s3.session-token"));
    Assertions.assertEquals(
        "v1/irc1/namespaces/db/tables/tbl/credentials",
        credential.config().get("client.refresh-credentials-endpoint"));
    Assertions.assertFalse(response.config().containsKey("client.refresh-credentials-endpoint"));
  }

  @Test
  void testRESTCatalogClientConfigMergesRemote() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "rest",
                IcebergConstants.URI,
                "http://client-config-only:8181"));

    RESTCatalog restCatalog = mock(RESTCatalog.class);
    when(restCatalog.properties())
        .thenReturn(
            ImmutableMap.of(
                IcebergConstants.URI,
                "http://merged-from-remote-config:9999",
                IcebergConstants.IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO",
                IcebergConstants.ICEBERG_S3_ENDPOINT,
                "http://localhost:9000",
                IcebergConstants.ICEBERG_ACCESS_DELEGATION,
                "vended-credentials",
                IcebergConstants.WAREHOUSE,
                "s3://remote/warehouse"));

    Map<String, String> configToClients =
        CatalogWrapperForREST.buildCatalogConfigToClients(config, restCatalog);

    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", configToClients.get(IcebergConstants.IO_IMPL));
    Assertions.assertEquals(
        "http://localhost:9000", configToClients.get(IcebergConstants.ICEBERG_S3_ENDPOINT));
    Assertions.assertEquals(
        "vended-credentials", configToClients.get(IcebergConstants.ICEBERG_ACCESS_DELEGATION));
  }

  @Test
  void testCatalogConfigToClientsIncludesResolvingFileIO() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "hive",
                IcebergConstants.IO_IMPL,
                ResolvingFileIO.class.getName(),
                IcebergConstants.WAREHOUSE,
                "s3://bucket/warehouse"));
    Catalog catalog = mock(Catalog.class);

    Map<String, String> configToClients =
        CatalogWrapperForREST.buildCatalogConfigToClients(config, catalog);

    Assertions.assertEquals(
        ResolvingFileIO.class.getName(), configToClients.get(IcebergConstants.IO_IMPL));
  }

  @Test
  void testNonRESTCatalogClientConfig() {
    Catalog catalog = mock(Catalog.class);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "hive",
                IcebergConstants.URI,
                "thrift://hive-metastore:9083",
                IcebergConstants.IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO"));
    Map<String, String> configToClients =
        CatalogWrapperForREST.buildCatalogConfigToClients(config, catalog);
    Assertions.assertFalse(configToClients.containsKey(IcebergConstants.URI));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", configToClients.get(IcebergConstants.IO_IMPL));
    Assertions.assertFalse(configToClients.containsKey(IcebergConstants.DATA_ACCESS));
  }

  @Test
  void testCatalogClientConfigRejectsBadDataAccess() {
    Catalog catalog = mock(Catalog.class);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "hive",
                IcebergConstants.ICEBERG_ACCESS_DELEGATION,
                "invalid-mode"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.buildCatalogConfigToClients(config, catalog));
  }

  @Test
  void testWrapperLazyLoadsCatalog() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));

    CONSTRUCTION_IN_PROGRESS.set(true);
    try {
      Assertions.assertDoesNotThrow(() -> new LazyCheckCatalogWrapperForREST("test", config));
    } finally {
      CONSTRUCTION_IN_PROGRESS.set(false);
    }
  }

  @Test
  void testStageCreateWithLocationIncludesFileIo() throws Exception {
    RESTCatalog catalog = mock(RESTCatalog.class);
    Catalog.TableBuilder tableBuilder = mock(Catalog.TableBuilder.class);
    Transaction transaction = mock(Transaction.class);
    Table table = mock(Table.class);
    FileIO fileIO = mock(FileIO.class);
    when(catalog.buildTable(any(TableIdentifier.class), any())).thenReturn(tableBuilder);
    when(tableBuilder.withPartitionSpec(any())).thenReturn(tableBuilder);
    when(tableBuilder.withSortOrder(any())).thenReturn(tableBuilder);
    when(tableBuilder.withProperties(anyMap())).thenReturn(tableBuilder);
    when(tableBuilder.withLocation("s3://bucket/warehouse/table")).thenReturn(tableBuilder);
    when(tableBuilder.createTransaction()).thenReturn(transaction);
    when(transaction.table()).thenReturn(table);
    when(table.io()).thenReturn(fileIO);
    when(table.location()).thenReturn("s3://bucket/warehouse/table");
    when(fileIO.properties())
        .thenReturn(
            ImmutableMap.of(
                IcebergConstants.IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO",
                IcebergConstants.ICEBERG_S3_ENDPOINT,
                "http://localhost:9000"));

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("test", config, catalog);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("tbl")
            .withSchema(schema)
            .withLocation("s3://bucket/warehouse/table")
            .stageCreate()
            .build();

    LoadTableResponse response = wrapper.createTable(Namespace.of("db"), request, false);

    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", response.config().get(IcebergConstants.IO_IMPL));
    Assertions.assertEquals(
        "http://localhost:9000", response.config().get(IcebergConstants.ICEBERG_S3_ENDPOINT));
    verify(tableBuilder).withLocation("s3://bucket/warehouse/table");
  }

  @Test
  void testStageCreateNullLocationSkipsWithLocation() {
    RESTCatalog catalog = mock(RESTCatalog.class);
    Catalog.TableBuilder tableBuilder = mock(Catalog.TableBuilder.class);
    Transaction transaction = mock(Transaction.class);
    Table table = mock(Table.class);
    FileIO fileIO = mock(FileIO.class);
    when(catalog.buildTable(any(TableIdentifier.class), any())).thenReturn(tableBuilder);
    when(tableBuilder.withPartitionSpec(any())).thenReturn(tableBuilder);
    when(tableBuilder.withSortOrder(any())).thenReturn(tableBuilder);
    when(tableBuilder.withProperties(anyMap())).thenReturn(tableBuilder);
    when(tableBuilder.createTransaction()).thenReturn(transaction);
    when(transaction.table()).thenReturn(table);
    when(table.io()).thenReturn(fileIO);
    when(table.location()).thenReturn("s3://bucket/warehouse/default-location");
    when(fileIO.properties())
        .thenReturn(
            ImmutableMap.of(
                IcebergConstants.IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO",
                IcebergConstants.ICEBERG_S3_ENDPOINT,
                "http://localhost:9000"));

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("test", config, catalog);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    CreateTableRequest request =
        CreateTableRequest.builder().withName("tbl").withSchema(schema).stageCreate().build();

    LoadTableResponse response = wrapper.createTable(Namespace.of("db"), request, false);

    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", response.config().get(IcebergConstants.IO_IMPL));
    Assertions.assertEquals(
        "http://localhost:9000", response.config().get(IcebergConstants.ICEBERG_S3_ENDPOINT));
    verify(tableBuilder, never()).withLocation(any());
  }

  @Test
  void testStagedCreateRejectsExtraRequirements() {
    RESTCatalog catalog = mock(RESTCatalog.class);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("test", config, catalog);

    UpdateRequirement extraRequirement = mock(UpdateRequirement.class);
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(new UpdateRequirement.AssertTableDoesNotExist(), extraRequirement),
            Collections.emptyList());

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> wrapper.updateTable(TableIdentifier.of("db", "tbl"), request));
  }

  @Test
  void testPostBuilderMetadataSkipsHandledKinds() {
    Schema schema = new Schema(Types.NestedField.required(1, "c", Types.LongType.get()));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.AddSchema(schema)));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.UpgradeFormatVersion(2)));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.SetCurrentSchema(-1)));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.SetLocation("file:///tmp/loc")));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.SetProperties(ImmutableMap.of("k", "v"))));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.RemoveProperties(Collections.singleton("k"))));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned())));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.SetDefaultPartitionSpec(PartitionSpec.unpartitioned().specId())));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.AddSortOrder(SortOrder.unsorted())));
    Assertions.assertFalse(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.SetDefaultSortOrder(SortOrder.unsorted().orderId())));
  }

  @Test
  void testPostBuilderMetadataAllowsAssignUuid() {
    Assertions.assertTrue(
        CatalogWrapperForREST.shouldApplyMetadataUpdateAfterBuilder(
            new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));
  }

  @Test
  void testStagedCreateBuilderUsesDerivedMetadataV3() {
    RESTCatalog catalog = mock(RESTCatalog.class);
    Catalog.TableBuilder tableBuilder = mock(Catalog.TableBuilder.class);
    BaseTransaction baseTransaction = mock(BaseTransaction.class);
    TableOperations ops = mock(TableOperations.class);
    when(catalog.buildTable(any(TableIdentifier.class), any(Schema.class)))
        .thenReturn(tableBuilder);
    when(tableBuilder.withPartitionSpec(any())).thenReturn(tableBuilder);
    when(tableBuilder.withSortOrder(any())).thenReturn(tableBuilder);
    when(tableBuilder.withLocation(any())).thenReturn(tableBuilder);
    when(tableBuilder.withProperty(anyString(), anyString())).thenReturn(tableBuilder);
    when(tableBuilder.withProperties(any())).thenReturn(tableBuilder);
    when(tableBuilder.createOrReplaceTransaction()).thenReturn(baseTransaction);
    when(baseTransaction.underlyingOps()).thenReturn(ops);
    when(baseTransaction.currentMetadata()).thenReturn(minimalTableMetadataForStagedCreateTest());

    AtomicReference<TableMetadata> opsCurrent = new AtomicReference<>();
    when(ops.current()).thenAnswer(invocation -> opsCurrent.get());
    doAnswer(
            invocation -> {
              opsCurrent.set(invocation.getArgument(1));
              return null;
            })
        .when(ops)
        .commit(any(), any());

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("test", config, catalog);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Optional<Integer> upgradeFormat = Optional.of(3);
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(new UpdateRequirement.AssertTableDoesNotExist()),
            stagedCreateMetadataUpdates(schema, upgradeFormat));

    Assertions.assertDoesNotThrow(
        () -> wrapper.updateTable(TableIdentifier.of("db", "tbl"), request));

    verify(tableBuilder).withPartitionSpec(any());
    verify(tableBuilder).withSortOrder(any());
    verify(tableBuilder)
        .withProperty(
            "format-version", expectedFormatVersionStringAfterStagedUpdates(schema, upgradeFormat));
    verify(tableBuilder).withProperties(any());
    verify(tableBuilder).createOrReplaceTransaction();
  }

  @Test
  void testStagedCreateSetsFormatVersionWhenNoUpgradeFormatUpdate() {
    RESTCatalog catalog = mock(RESTCatalog.class);
    Catalog.TableBuilder tableBuilder = mock(Catalog.TableBuilder.class);
    BaseTransaction baseTransaction = mock(BaseTransaction.class);
    TableOperations ops = mock(TableOperations.class);
    when(catalog.buildTable(any(TableIdentifier.class), any(Schema.class)))
        .thenReturn(tableBuilder);
    when(tableBuilder.withPartitionSpec(any())).thenReturn(tableBuilder);
    when(tableBuilder.withSortOrder(any())).thenReturn(tableBuilder);
    when(tableBuilder.withLocation(any())).thenReturn(tableBuilder);
    when(tableBuilder.withProperty(anyString(), anyString())).thenReturn(tableBuilder);
    when(tableBuilder.withProperties(any())).thenReturn(tableBuilder);
    when(tableBuilder.createOrReplaceTransaction()).thenReturn(baseTransaction);
    when(baseTransaction.underlyingOps()).thenReturn(ops);
    when(baseTransaction.currentMetadata()).thenReturn(minimalTableMetadataForStagedCreateTest());

    AtomicReference<TableMetadata> opsCurrent = new AtomicReference<>();
    when(ops.current()).thenAnswer(invocation -> opsCurrent.get());
    doAnswer(
            invocation -> {
              opsCurrent.set(invocation.getArgument(1));
              return null;
            })
        .when(ops)
        .commit(any(), any());

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    CatalogWrapperForREST wrapper = new StaticCatalogWrapperForREST("test", config, catalog);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Optional<Integer> noExplicitUpgrade = Optional.empty();
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(new UpdateRequirement.AssertTableDoesNotExist()),
            stagedCreateMetadataUpdates(schema, noExplicitUpgrade));

    Assertions.assertDoesNotThrow(
        () -> wrapper.updateTable(TableIdentifier.of("db", "tbl"), request));

    verify(tableBuilder)
        .withProperty(
            "format-version",
            expectedFormatVersionStringAfterStagedUpdates(schema, noExplicitUpgrade));
  }

  /**
   * Same derivation as {@link CatalogWrapperForREST#tableUpdateInternal} for staged create: replay
   * metadata updates and read {@link TableMetadata#formatVersion()}.
   */
  private static String expectedFormatVersionStringAfterStagedUpdates(
      Schema schema, Optional<Integer> formatVersionForUpgrade) {
    List<MetadataUpdate> updates = stagedCreateMetadataUpdates(schema, formatVersionForUpgrade);
    Optional<Integer> formatVersion =
        updates.stream()
            .filter(update -> update instanceof MetadataUpdate.UpgradeFormatVersion)
            .map(update -> ((MetadataUpdate.UpgradeFormatVersion) update).formatVersion())
            .findFirst();
    TableMetadata.Builder changedMetadata =
        formatVersion.map(TableMetadata::buildFromEmpty).orElse(TableMetadata.buildFromEmpty());
    updates.forEach(update -> update.applyTo(changedMetadata));
    return String.valueOf(changedMetadata.build().formatVersion());
  }

  /**
   * Minimal valid Iceberg staged-create update sequence so {@link TableMetadata.Builder#build()}
   * succeeds in {@link CatalogWrapperForREST#tableUpdateInternal}.
   */
  private static List<MetadataUpdate> stagedCreateMetadataUpdates(
      Schema schema, Optional<Integer> formatVersion) {
    List<MetadataUpdate> updates = new ArrayList<>();
    updates.add(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()));
    formatVersion.ifPresent(v -> updates.add(new MetadataUpdate.UpgradeFormatVersion(v)));
    updates.add(new MetadataUpdate.AddSchema(schema));
    updates.add(new MetadataUpdate.SetCurrentSchema(-1));
    PartitionSpec spec = PartitionSpec.unpartitioned();
    updates.add(new MetadataUpdate.AddPartitionSpec(spec));
    updates.add(new MetadataUpdate.SetDefaultPartitionSpec(spec.specId()));
    SortOrder sortOrder = SortOrder.unsorted();
    updates.add(new MetadataUpdate.AddSortOrder(sortOrder));
    updates.add(new MetadataUpdate.SetDefaultSortOrder(sortOrder.orderId()));
    updates.add(new MetadataUpdate.SetLocation("file:///tmp/t"));
    return updates;
  }

  private static TableMetadata minimalTableMetadataForStagedCreateTest() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        "file:///tmp/t",
        Collections.emptyMap());
  }

  private static class LazyCheckCatalogWrapperForREST extends CatalogWrapperForREST {

    LazyCheckCatalogWrapperForREST(String catalogName, IcebergConfig config) {
      super(catalogName, config);
    }

    @Override
    public Catalog getCatalog() {
      if (CONSTRUCTION_IN_PROGRESS.get()) {
        throw new AssertionError("Catalog should not be loaded during wrapper construction");
      }
      return super.getCatalog();
    }
  }

  private static class StaticCatalogWrapperForREST extends CatalogWrapperForREST {
    private final Catalog catalog;

    StaticCatalogWrapperForREST(String catalogName, IcebergConfig config, Catalog catalog) {
      super(catalogName, config);
      this.catalog = catalog;
    }

    @Override
    public Catalog getCatalog() {
      return catalog;
    }
  }
}
