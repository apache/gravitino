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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

/**
 * Tests that delegation token collection resolves the configured filesets first, and that only
 * tokens owned by the current user are handed out.
 */
public class TestPrepareDelegationTokens {

  private static final String METALAKE = "ml";
  private static final Path GVFS_PATH = new Path("gvfs://fileset/catalog/schema/fs");
  private static final Path OTHER_PATH = new Path("gvfs://fileset/catalog/schema/other");
  private static final Text SERVICE = new Text("nn:8020");

  @Test
  public void testResolvesConfiguredFilesets() throws Exception {
    Catalog catalog = newCatalog();
    Fileset fileset = newFileset();
    Fileset other = newFileset();
    TestOps ops = newOps(withFilesets(GVFS_PATH + ", " + OTHER_PATH), catalog, fileset);
    when(catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "other")))
        .thenReturn(other);

    ops.addDelegationTokens("renewer", new Credentials());

    assertEquals(
        List.of(location(fileset), location(other)).toString(), ops.resolvedPaths.toString());
  }

  @Test
  public void testDoesNotCreateMissingLocation() throws Exception {
    java.nio.file.Path missing = Files.createTempDirectory("gvfs-token-missing").resolve("fs");
    String location = missing.toUri().toString();
    Fileset fileset = mock(Fileset.class);
    when(fileset.properties())
        .thenReturn(ImmutableMap.of(Fileset.PROPERTY_DEFAULT_LOCATION_NAME, "unknown"));
    when(fileset.storageLocations()).thenReturn(ImmutableMap.of("unknown", location));
    Catalog catalog = newCatalog();
    when(catalog.properties()).thenReturn(ImmutableMap.of("disable-filesystem-ops", "true"));
    TestOps ops = newOps(withFilesets(GVFS_PATH.toString()), catalog, fileset);

    ops.addDelegationTokens("renewer", new Credentials());

    assertEquals(List.of(new Path(location)), ops.resolvedPaths);
    assertFalse(Files.exists(missing));
  }

  @Test
  public void testResolvesNothingWithoutConfiguredFilesets() throws Exception {
    TestOps ops = newOps(newConf(), newCatalog(), newFileset());

    ops.addDelegationTokens("renewer", new Credentials());

    assertTrue(ops.resolvedPaths.isEmpty());
  }

  @Test
  public void testResolvesTheCurrentLocation() throws Exception {
    String defaultLocation = Files.createTempDirectory("gvfs-token-default").toUri().toString();
    String currentLocation = Files.createTempDirectory("gvfs-token-current").toUri().toString();
    Fileset fileset = mock(Fileset.class);
    when(fileset.properties())
        .thenReturn(ImmutableMap.of(Fileset.PROPERTY_DEFAULT_LOCATION_NAME, "unknown"));
    when(fileset.storageLocations())
        .thenReturn(ImmutableMap.of("unknown", defaultLocation, "current", currentLocation));

    Configuration conf = withFilesets(GVFS_PATH.toString());
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME, "current");
    TestOps ops = newOps(conf, newCatalog(), fileset);

    ops.addDelegationTokens("renewer", new Credentials());

    assertEquals(List.of(new Path(currentLocation)), ops.resolvedPaths);
  }

  @Test
  public void testUnresolvableFilesetDoesNotStopOthers() throws Exception {
    Catalog catalog = newCatalog();
    Fileset other = newFileset();
    when(catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "fs")))
        .thenThrow(new NoSuchFilesetException("gone"));
    when(catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "other")))
        .thenReturn(other);
    TestOps ops = newOps(withFilesets(GVFS_PATH + "," + OTHER_PATH), catalog, null);

    assertDoesNotThrow(() -> ops.addDelegationTokens("renewer", new Credentials()));
    assertEquals(List.of(location(other)).toString(), ops.resolvedPaths.toString());
  }

  @Test
  public void testCollectsTokensForFilesetsEvictedFromCache() throws Exception {
    Catalog catalog = newCatalog();
    Fileset fileset = newFileset();
    Fileset other = newFileset();
    Configuration conf = withFilesets(GVFS_PATH + "," + OTHER_PATH);
    conf.setInt(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY, 1);
    TestOps ops = newOps(conf, catalog, fileset);
    when(catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "other")))
        .thenReturn(other);
    // Each fileset lives on its own cluster, so the second one evicts the first from the cache.
    ops.fakeFileSystems = true;

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");
    Credentials credentials = new Credentials();
    ugi.doAs(
        (PrivilegedExceptionAction<Token<?>[]>)
            () -> ops.addDelegationTokens("renewer", credentials));

    assertEquals(1, ops.internalFileSystemCache().estimatedSize());
    assertEquals(2, credentials.numberOfTokens());
  }

  @Test
  public void testAddsTokenOwnedByCurrentUser() throws Exception {
    Token<?> token = newToken(UserGroupInformation.createRemoteUser("alice"));
    Credentials credentials = collectAs("alice", token);

    assertSame(token, credentials.getToken(SERVICE));
  }

  @Test
  public void testSkipsTokenOwnedByAnotherUser() throws Exception {
    // A fileset with its own Kerberos principal is accessed as that principal.
    Token<?> token = newToken(UserGroupInformation.createRemoteUser("svc"));
    Credentials credentials = collectAs("alice", token);

    assertNull(credentials.getToken(SERVICE));
  }

  @Test
  public void testAddsImpersonatedToken() throws Exception {
    UserGroupInformation proxy =
        UserGroupInformation.createProxyUser("alice", UserGroupInformation.createRemoteUser("svc"));
    Token<?> token = newToken(proxy);
    Credentials credentials = collectAs("alice", token);

    assertSame(token, credentials.getToken(SERVICE));
  }

  @Test
  public void testAcceptsNullCredentials() throws Exception {
    Token<?> token = newToken(UserGroupInformation.createRemoteUser("alice"));

    assertArrayEquals(new Token<?>[] {token}, addTokensAs("alice", token, null));
  }

  @Test
  public void testDoesNotReplaceExistingToken() throws Exception {
    Token<?> existing = newToken(UserGroupInformation.createRemoteUser("alice"));
    Credentials credentials = new Credentials();
    credentials.addToken(SERVICE, existing);

    collectAs("alice", newToken(UserGroupInformation.createRemoteUser("alice")), credentials);

    assertSame(existing, credentials.getToken(SERVICE));
  }

  private Credentials collectAs(String user, Token<?> token) throws Exception {
    return collectAs(user, token, new Credentials());
  }

  private Credentials collectAs(String user, Token<?> token, Credentials credentials)
      throws Exception {
    addTokensAs(user, token, credentials);
    return credentials;
  }

  private Token<?>[] addTokensAs(String user, Token<?> token, Credentials credentials)
      throws Exception {
    FileSystem fs = mock(FileSystem.class);
    when(fs.addDelegationTokens(any(), any()))
        .thenAnswer(
            invocation -> {
              Credentials target = invocation.getArgument(1);
              if (target.getToken(SERVICE) == null) {
                target.addToken(SERVICE, token);
              }
              return new Token<?>[] {token};
            });

    TestOps ops = newOps(newConf(), newCatalog(), null);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ops.internalFileSystemCache()
        .put(new BaseGVFSOperations.FileSystemCacheKey("hdfs", "nn:8020", ugi), fs);
    return ugi.doAs(
        (PrivilegedExceptionAction<Token<?>[]>)
            () -> ops.addDelegationTokens("renewer", credentials));
  }

  private static FileSystem newTokenIssuingFileSystem(Text service) {
    FileSystem fs = mock(FileSystem.class);
    try {
      Token<?> token = newToken(UserGroupInformation.getCurrentUser());
      when(fs.addDelegationTokens(any(), any()))
          .thenAnswer(
              invocation -> {
                Credentials target = invocation.getArgument(1);
                target.addToken(service, token);
                return new Token<?>[] {token};
              });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return fs;
  }

  @SuppressWarnings("unchecked")
  private static Token<?> newToken(UserGroupInformation owner) throws IOException {
    AbstractDelegationTokenIdentifier identifier = mock(AbstractDelegationTokenIdentifier.class);
    when(identifier.getUser()).thenReturn(owner);
    Token<AbstractDelegationTokenIdentifier> token = mock(Token.class);
    when(token.decodeIdentifier()).thenReturn(identifier);
    return token;
  }

  private Path location(Fileset fileset) {
    return new Path(fileset.storageLocations().get("unknown"));
  }

  private Fileset newFileset() throws IOException {
    String location = Files.createTempDirectory("gvfs-token-test").toUri().toString();
    Fileset fileset = mock(Fileset.class);
    when(fileset.properties())
        .thenReturn(ImmutableMap.of(Fileset.PROPERTY_DEFAULT_LOCATION_NAME, "unknown"));
    when(fileset.storageLocations()).thenReturn(ImmutableMap.of("unknown", location));
    return fileset;
  }

  /** The catalog is both a {@link Catalog} and a {@link FilesetCatalog}, as it is on the server. */
  private Catalog newCatalog() {
    Catalog catalog = mock(Catalog.class, withSettings().extraInterfaces(FilesetCatalog.class));
    Schema schema = mock(Schema.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);

    when(catalog.properties()).thenReturn(ImmutableMap.of());
    when(catalog.asSchemas()).thenReturn(schemas);
    when(catalog.asFilesetCatalog()).thenReturn((FilesetCatalog) catalog);
    when(schemas.loadSchema("schema")).thenReturn(schema);
    when(schema.properties()).thenReturn(ImmutableMap.of());
    return catalog;
  }

  private Configuration withFilesets(String filesets) {
    Configuration conf = newConf();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_DELEGATION_TOKEN_FILESETS, filesets);
    return conf;
  }

  private Configuration newConf() {
    Configuration conf = new Configuration();
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, METALAKE);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");
    return conf;
  }

  private TestOps newOps(Configuration conf, Catalog catalog, Fileset fileset) throws Exception {
    if (fileset != null) {
      when(catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "fs")))
          .thenReturn(fileset);
    }

    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("catalog")).thenReturn(catalog);

    return new TestOps(conf, client);
  }

  private static final class TestOps extends BaseGVFSOperations {
    private final GravitinoClient client;
    private final List<Path> resolvedPaths = new ArrayList<>();
    private boolean fakeFileSystems;

    private TestOps(Configuration configuration, GravitinoClient client) {
      super(configuration);
      this.client = client;
    }

    @Override
    GravitinoClient getGravitinoClient() {
      return client;
    }

    @Override
    protected FileSystem getActualFileSystemByPath(
        Path actualFilePath, Map<String, String> allProperties) {
      resolvedPaths.add(actualFilePath);
      if (!fakeFileSystems) {
        return super.getActualFileSystemByPath(actualFilePath, allProperties);
      }
      FileSystem fs = newTokenIssuingFileSystem(new Text(actualFilePath.toString()));
      try {
        internalFileSystemCache()
            .put(
                new FileSystemCacheKey(
                    "hdfs", actualFilePath.toString(), UserGroupInformation.getCurrentUser()),
                fs);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      internalFileSystemCache().cleanUp();
      return fs;
    }

    @Override
    protected Schema getSchema(NameIdentifier schemaIdent) {
      Schema schema = mock(Schema.class);
      when(schema.properties()).thenReturn(ImmutableMap.of());
      return schema;
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
        Progressable progress) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path gvfsPath, int bufferSize, Progressable progress) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path srcGvfsPath, Path dstGvfsPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path gvfsPath, boolean recursive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path gvfsPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path gvfsPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path gvfsPath, FsPermission permission) {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getDefaultReplication(Path gvfsPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getDefaultBlockSize(Path gvfsPath) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
      return addDelegationTokensForAllFS(renewer, credentials);
    }
  }
}
