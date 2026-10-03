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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
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
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

/**
 * Tests that delegation token collection resolves the fileset first, so that a token consumer such
 * as Spark, which collects tokens before anything has been read, does not end up with none.
 */
public class TestPrepareDelegationTokens {

  private static final String METALAKE = "ml";
  private static final Path GVFS_PATH = new Path("gvfs://fileset/catalog/schema/fs");

  @Test
  public void testResolvesTheRequestedFileset() throws Exception {
    Catalog catalog = newCatalog();
    Fileset fileset = newFileset();
    Fileset other = newFileset();
    TestOps ops = newOps(newConf(), catalog, fileset);
    when(catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("schema", "other")))
        .thenReturn(other);

    // The cache is shared by every fileset, so holding another one says nothing about this one.
    ops.prepareDelegationTokens(new Path("gvfs://fileset/catalog/schema/other"));
    ops.prepareDelegationTokens(GVFS_PATH);

    assertEquals(
        List.of(location(other), location(fileset)).toString(), ops.resolvedPaths.toString());
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

    Configuration conf = newConf();
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME, "current");
    TestOps ops = newOps(conf, newCatalog(), fileset);

    ops.prepareDelegationTokens(GVFS_PATH);

    assertEquals(1, ops.resolvedPaths.size());
    assertEquals(new Path(currentLocation), ops.resolvedPaths.get(0));
  }

  @Test
  public void testUnresolvableFilesetDoesNotThrow() throws Exception {
    Catalog catalog = newCatalog();
    when(catalog.asFilesetCatalog().loadFileset(any()))
        .thenThrow(new NoSuchFilesetException("gone"));
    TestOps ops = newOps(newConf(), catalog, null);

    assertDoesNotThrow(() -> ops.prepareDelegationTokens(GVFS_PATH));
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
      return super.getActualFileSystemByPath(actualFilePath, allProperties);
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
