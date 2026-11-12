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
package org.apache.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.storage.CloudStorageCredentialPropertyKeys;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

public class TestBaseGVFSOperationsSecrets {

  @Test
  public void testMergeSecrets() throws Exception {
    Configuration conf = new Configuration();
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "ml");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");

    Catalog catalog = mock(Catalog.class);
    Schema schema = mock(Schema.class);
    Fileset fileset = mock(Fileset.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    FilesetCatalog filesetCatalog = mock(FilesetCatalog.class);
    SupportsSecrets catalogSecrets = mock(SupportsSecrets.class);
    SupportsSecrets schemaSecrets = mock(SupportsSecrets.class);
    SupportsSecrets filesetSecrets = mock(SupportsSecrets.class);

    when(catalog.properties()).thenReturn(Map.of("c-vis", "1"));
    when(catalog.supportsSecrets()).thenReturn(catalogSecrets);
    when(catalogSecrets.getSecrets()).thenReturn(Map.of("c-secret", "cs"));
    when(catalog.asSchemas()).thenReturn(schemas);
    when(schemas.loadSchema("schema")).thenReturn(schema);
    when(schema.properties()).thenReturn(Map.of("s-vis", "2"));
    when(schema.supportsSecrets()).thenReturn(schemaSecrets);
    when(schemaSecrets.getSecrets()).thenReturn(Map.of("s-secret", "ss"));
    when(catalog.asFilesetCatalog()).thenReturn(filesetCatalog);
    when(filesetCatalog.loadFileset(NameIdentifier.of("schema", "fs"))).thenReturn(fileset);
    when(fileset.properties()).thenReturn(Map.of("f-vis", "3"));
    when(fileset.supportsSecrets()).thenReturn(filesetSecrets);
    when(filesetSecrets.getSecrets()).thenReturn(Map.of("f-secret", "fs"));

    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("catalog")).thenReturn(catalog);

    TestOps ops = new TestOps(conf, client);
    Map<String, String> all =
        ops.getAllProperties(NameIdentifier.of("ml", "catalog", "schema", "fs"));

    assertEquals("1", all.get("c-vis"));
    assertEquals("cs", all.get("c-secret"));
    assertEquals("2", all.get("s-vis"));
    assertEquals("ss", all.get("s-secret"));
    assertEquals("3", all.get("f-vis"));
    assertEquals("fs", all.get("f-secret"));
  }

  @Test
  public void testSecretOverride() throws Exception {
    Configuration conf = new Configuration();
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "ml");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");

    Catalog catalog = mock(Catalog.class);
    Schema schema = mock(Schema.class);
    Fileset fileset = mock(Fileset.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    FilesetCatalog filesetCatalog = mock(FilesetCatalog.class);
    SupportsSecrets catalogSecrets = mock(SupportsSecrets.class);
    SupportsSecrets schemaSecrets = mock(SupportsSecrets.class);
    SupportsSecrets filesetSecrets = mock(SupportsSecrets.class);

    when(catalog.properties()).thenReturn(Map.of("shared", "from-catalog-props"));
    when(catalog.supportsSecrets()).thenReturn(catalogSecrets);
    when(catalogSecrets.getSecrets()).thenReturn(Map.of("shared", "from-catalog-secret"));
    when(catalog.asSchemas()).thenReturn(schemas);
    when(schemas.loadSchema("schema")).thenReturn(schema);
    when(schema.properties()).thenReturn(Map.of("shared", "from-schema-props"));
    when(schema.supportsSecrets()).thenReturn(schemaSecrets);
    when(schemaSecrets.getSecrets()).thenReturn(Map.of("shared", "from-schema-secret"));
    when(catalog.asFilesetCatalog()).thenReturn(filesetCatalog);
    when(filesetCatalog.loadFileset(NameIdentifier.of("schema", "fs"))).thenReturn(fileset);
    when(fileset.properties()).thenReturn(Map.of("shared", "from-fileset-props"));
    when(fileset.supportsSecrets()).thenReturn(filesetSecrets);
    when(filesetSecrets.getSecrets()).thenReturn(Map.of("shared", "from-fileset-secret"));

    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("catalog")).thenReturn(catalog);

    TestOps ops = new TestOps(conf, client);
    Map<String, String> all =
        ops.getAllProperties(NameIdentifier.of("ml", "catalog", "schema", "fs"));

    assertEquals("from-fileset-secret", all.get("shared"));
  }

  @Test
  public void testNullProps() throws Exception {
    Configuration conf = new Configuration();
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "ml");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");

    Catalog catalog = mock(Catalog.class);
    Schema schema = mock(Schema.class);
    Fileset fileset = mock(Fileset.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    FilesetCatalog filesetCatalog = mock(FilesetCatalog.class);
    SupportsSecrets catalogSecrets = mock(SupportsSecrets.class);
    SupportsSecrets schemaSecrets = mock(SupportsSecrets.class);
    SupportsSecrets filesetSecrets = mock(SupportsSecrets.class);

    when(catalog.properties()).thenReturn(null);
    when(catalog.supportsSecrets()).thenReturn(catalogSecrets);
    when(catalogSecrets.getSecrets()).thenReturn(Map.of("c-secret", "cs"));
    when(catalog.asSchemas()).thenReturn(schemas);
    when(schemas.loadSchema("schema")).thenReturn(schema);
    when(schema.properties()).thenReturn(null);
    when(schema.supportsSecrets()).thenReturn(schemaSecrets);
    when(schemaSecrets.getSecrets()).thenReturn(Map.of("s-secret", "ss"));
    when(catalog.asFilesetCatalog()).thenReturn(filesetCatalog);
    when(filesetCatalog.loadFileset(NameIdentifier.of("schema", "fs"))).thenReturn(fileset);
    when(fileset.properties()).thenReturn(null);
    when(fileset.supportsSecrets()).thenReturn(filesetSecrets);
    when(filesetSecrets.getSecrets()).thenReturn(Map.of("f-secret", "fs"));

    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("catalog")).thenReturn(catalog);

    TestOps ops = new TestOps(conf, client);
    Map<String, String> all =
        ops.getAllProperties(NameIdentifier.of("ml", "catalog", "schema", "fs"));

    assertEquals("cs", all.get("c-secret"));
    assertEquals("ss", all.get("s-secret"));
    assertEquals("fs", all.get("f-secret"));
  }

  @Test
  public void testOmitsStaticCredentialKeysFromRestProperties() throws Exception {
    Configuration conf = new Configuration();
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "ml");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");
    conf.set(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "local-access-key");

    Catalog catalog = mock(Catalog.class);
    Schema schema = mock(Schema.class);
    Fileset fileset = mock(Fileset.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    FilesetCatalog filesetCatalog = mock(FilesetCatalog.class);

    when(catalog.properties())
        .thenReturn(
            Map.of(
                S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                "******",
                S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
                "secret-from-rest",
                "s3-endpoint",
                "http://s3.example.com"));
    when(catalog.supportsSecrets()).thenReturn(null);
    when(catalog.asSchemas()).thenReturn(schemas);
    when(schemas.loadSchema("schema")).thenReturn(schema);
    when(schema.properties()).thenReturn(Map.of());
    when(schema.supportsSecrets()).thenReturn(null);
    when(catalog.asFilesetCatalog()).thenReturn(filesetCatalog);
    when(filesetCatalog.loadFileset(NameIdentifier.of("schema", "fs"))).thenReturn(fileset);
    when(fileset.properties()).thenReturn(Map.of());
    when(fileset.supportsSecrets()).thenReturn(null);

    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("catalog")).thenReturn(catalog);

    TestOps ops = new TestOps(conf, client);
    Map<String, String> all =
        ops.getAllProperties(NameIdentifier.of("ml", "catalog", "schema", "fs"));

    assertFalse(all.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertEquals("http://s3.example.com", all.get("s3-endpoint"));
    assertEquals("local-access-key", all.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse("******".equals(all.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID)));
  }

  @Test
  public void testRecoversAccessKeyPairFromGetCredentialsWhenPropertiesAreMasked()
      throws Exception {
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    when(credentials.getCredentials())
        .thenReturn(
            new Credential[] {new S3SecretKeyCredential("AKIATEST", "secret-from-getCredentials")});
    Catalog catalog =
        mockCatalogWithCredentials(
            Map.of(
                S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                CloudStorageCredentialPropertyKeys.MASKED_PROPERTY_VALUE,
                S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
                CloudStorageCredentialPropertyKeys.MASKED_PROPERTY_VALUE,
                "s3-endpoint",
                "http://s3.example.com"),
            credentials);

    Map<String, String> all = getAllProperties(catalog);

    assertEquals("AKIATEST", all.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals(
        "secret-from-getCredentials", all.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertEquals("http://s3.example.com", all.get("s3-endpoint"));
  }

  @Test
  void testCatalogCredentialDenialDoesNotBlockFilesetCredentials() throws Exception {
    SupportsCredentials catalogCredentials = mock(SupportsCredentials.class);
    when(catalogCredentials.getCredentials()).thenThrow(new ForbiddenException("catalog denied"));
    Catalog catalog =
        mockCatalogWithCredentials(
            Map.of("s3-endpoint", "http://s3.example.com"), catalogCredentials);
    Fileset fileset = catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "fs"));
    when(fileset.storageLocations()).thenReturn(Map.of("default", "s3a://bucket/data"));
    SupportsCredentials scopedCredentials = mock(SupportsCredentials.class);
    when(fileset.supportsCredentials()).thenReturn(scopedCredentials);
    when(scopedCredentials.getCredentials())
        .thenReturn(
            new Credential[] {
              new S3TokenCredential("SCOPED", "scoped-secret", "token", Long.MAX_VALUE)
            });
    TestOps ops = spy(opsFor(catalog));
    NameIdentifier ident = NameIdentifier.of("ml", "catalog", "schema", "fs");
    doReturn(fileset).when(ops).getFileset(ident);
    doReturn(true).when(ops).enableCredentialVending();
    FileSystem filesystem = mock(FileSystem.class);
    doReturn(filesystem).when(ops).getActualFileSystemByPath(any(Path.class), anyMap());

    assertSame(filesystem, ops.getActualFileSystemByLocationName(ident, "default"));
    verify(scopedCredentials).getCredentials();

    ForbiddenException denied = new ForbiddenException("fileset denied");
    when(scopedCredentials.getCredentials()).thenThrow(denied);
    RuntimeException failure =
        assertThrows(
            RuntimeException.class, () -> ops.getActualFileSystemByLocationName(ident, "default"));
    assertSame(denied, failure.getCause());
  }

  @Test
  public void testRestFailureDoesNotCacheEmptyStaticCredentials() throws Exception {
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    when(credentials.getCredentials())
        .thenThrow(new RESTException("transient failure"))
        .thenReturn(new Credential[] {new S3SecretKeyCredential("AKIATEST", "secret-after-retry")});
    Catalog catalog =
        mockCatalogWithCredentials(Map.of("s3-endpoint", "http://s3.example.com"), credentials);

    TestOps ops = opsFor(catalog);
    NameIdentifier ident = NameIdentifier.of("ml", "catalog", "schema", "fs");

    Map<String, String> first = ops.getAllProperties(ident);
    assertFalse(first.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals("http://s3.example.com", first.get("s3-endpoint"));

    Map<String, String> second = ops.getAllProperties(ident);
    assertEquals("AKIATEST", second.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals("secret-after-retry", second.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  public void testSkipsExpiringCredentialsFromGetCredentials() throws Exception {
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    when(credentials.getCredentials())
        .thenReturn(
            new Credential[] {
              new S3TokenCredential("SESSION", "session-secret", "tok", 1_700_000_000_000L),
              new S3SecretKeyCredential("AKIATEST", "static-secret")
            });
    Catalog catalog =
        mockCatalogWithCredentials(Map.of("s3-endpoint", "http://s3.example.com"), credentials);

    Map<String, String> all = getAllProperties(catalog);

    assertEquals("AKIATEST", all.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals("static-secret", all.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertFalse(all.containsKey("s3-session-token"));
  }

  private static Map<String, String> getAllProperties(Catalog catalog) throws Exception {
    return opsFor(catalog).getAllProperties(NameIdentifier.of("ml", "catalog", "schema", "fs"));
  }

  private static TestOps opsFor(Catalog catalog) {
    Configuration conf = new Configuration();
    conf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_AUTO_CREATE_LOCATION, false);
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "ml");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("catalog")).thenReturn(catalog);
    return new TestOps(conf, client);
  }

  /** Mocks catalog/schema/fileset chain with the given catalog props and credentials. */
  private static Catalog mockCatalogWithCredentials(
      Map<String, String> catalogProps, SupportsCredentials credentials) {
    Catalog catalog = mock(Catalog.class);
    Schema schema = mock(Schema.class);
    Fileset fileset = mock(Fileset.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    FilesetCatalog filesetCatalog = mock(FilesetCatalog.class);

    when(catalog.name()).thenReturn("catalog");
    when(catalog.properties()).thenReturn(catalogProps);
    when(catalog.supportsSecrets()).thenReturn(null);
    when(catalog.supportsCredentials()).thenReturn(credentials);
    when(catalog.asSchemas()).thenReturn(schemas);
    when(schemas.loadSchema("schema")).thenReturn(schema);
    when(schema.properties()).thenReturn(Map.of());
    when(schema.supportsSecrets()).thenReturn(null);
    when(catalog.asFilesetCatalog()).thenReturn(filesetCatalog);
    when(filesetCatalog.loadFileset(NameIdentifier.of("schema", "fs"))).thenReturn(fileset);
    when(fileset.properties()).thenReturn(Map.of());
    when(fileset.supportsSecrets()).thenReturn(null);
    return catalog;
  }

  private static class TestOps extends BaseGVFSOperations {
    private final GravitinoClient client;

    private TestOps(Configuration configuration, GravitinoClient client) {
      super(configuration);
      this.client = client;
    }

    @Override
    GravitinoClient getGravitinoClient() {
      return client;
    }

    @Override
    public FSDataInputStream open(Path gvfsPath, int bufferSize) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path gvfsDir) {}

    @Override
    public FSDataOutputStream create(
        Path gvfsPath,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path gvfsPath, int bufferSize, Progressable progress) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path srcGvfsPath, Path dstGvfsPath) {
      return false;
    }

    @Override
    public boolean delete(Path gvfsPath, boolean recursive) {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path gvfsPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path gvfsPath) {
      return new FileStatus[0];
    }

    @Override
    public boolean mkdirs(Path gvfsPath, FsPermission permission) {
      return false;
    }

    @Override
    public short getDefaultReplication(Path gvfsPath) {
      return 1;
    }

    @Override
    public long getDefaultBlockSize(Path gvfsPath) {
      return 1L;
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
      return new Token<?>[0];
    }

    @Override
    public void close() {}
  }
}
