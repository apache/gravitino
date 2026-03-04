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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Optional;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

/**
 * Tests for lazy loading functionality in {@link BaseGVFSOperations}, verifying that
 * GravitinoClient and FilesetMetadataCache are NOT created during construction and remain null
 * until first access. These tests use reflection to verify the lazy initialization pattern without
 * requiring an actual Gravitino server.
 */
public class TestClientAndCacheLazyLoadingBehavior {

  /** A minimal concrete implementation of BaseGVFSOperations for testing lazy loading behavior. */
  private static class TestableGVFSOperations extends BaseGVFSOperations {
    protected TestableGVFSOperations(Configuration configuration) {
      super(configuration);
    }

    // Minimal implementations - not used in lazy loading tests
    @Override
    public FSDataInputStream open(Path gvfsPath, int bufferSize) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setWorkingDirectory(Path gvfsDir) {
      throw new UnsupportedOperationException("Not implemented");
    }

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
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public FSDataOutputStream append(Path gvfsPath, int bufferSize, Progressable progress)
        throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean rename(Path srcGvfsPath, Path dstGvfsPath) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean delete(Path gvfsPath, boolean recursive) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public FileStatus getFileStatus(Path gvfsPath) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public FileStatus[] listStatus(Path gvfsPath) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean mkdirs(Path gvfsPath, FsPermission permission) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public short getDefaultReplication(Path gvfsPath) {
      return 1;
    }

    @Override
    public long getDefaultBlockSize(Path gvfsPath) {
      return 134217728L;
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
      return new Token<?>[0];
    }
  }

  private Configuration createTestConfiguration() {
    Configuration conf = new Configuration();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "test_metalake");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");
    return conf;
  }

  /**
   * Test that GravitinoClient is NOT created during BaseGVFSOperations construction.
   *
   * <p>This verifies the lazy initialization behavior - the expensive client creation should be
   * deferred until first use.
   */
  @Test
  public void testClientNotCreatedDuringConstruction() throws Exception {
    Configuration conf = createTestConfiguration();
    TestableGVFSOperations operations = new TestableGVFSOperations(conf);

    // Access the private gravitinoClient field via reflection
    Field clientField = BaseGVFSOperations.class.getDeclaredField("gravitinoClient");
    clientField.setAccessible(true);
    GravitinoClient client = (GravitinoClient) clientField.get(operations);

    // Client should be null - not yet initialized
    assertNull(client, "GravitinoClient should not be created during construction");

    operations.close();
  }

  /**
   * Test that FilesetMetadataCache is NOT created during construction.
   *
   * <p>This verifies lazy initialization of the cache.
   */
  @Test
  public void testCacheNotCreatedDuringConstruction() throws Exception {
    Configuration conf = createTestConfiguration();
    conf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_METADATA_CACHE_ENABLE, true);

    TestableGVFSOperations operations = new TestableGVFSOperations(conf);

    // Access the private filesetMetadataCache field
    Field cacheField = BaseGVFSOperations.class.getDeclaredField("filesetMetadataCache");
    cacheField.setAccessible(true);
    Optional<?> cache = (Optional<?>) cacheField.get(operations);

    // Cache should be null - not yet initialized
    assertNull(cache, "FilesetMetadataCache should not be created during construction");

    operations.close();
  }

  /**
   * Test that FilesetMetadataCache returns empty Optional when disabled.
   *
   * <p>This verifies that when disabled, the cache initialization creates an empty Optional rather
   * than attempting to create a cache with a client.
   */
  @Test
  public void testCacheNotCreatedWhenDisabled() throws Exception {
    Configuration conf = createTestConfiguration();
    conf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_METADATA_CACHE_ENABLE, false);

    TestableGVFSOperations operations = new TestableGVFSOperations(conf);

    // Access the cache - should return empty without trying to create a client
    Optional<FilesetMetadataCache> cache = operations.getFilesetMetadataCache();

    assertFalse(cache.isPresent(), "FilesetMetadataCache should not be created when disabled");

    // Verify the field is now set (to Optional.empty())
    Field cacheField = BaseGVFSOperations.class.getDeclaredField("filesetMetadataCache");
    cacheField.setAccessible(true);
    Optional<?> fieldCache = (Optional<?>) cacheField.get(operations);
    assertNotNull(fieldCache, "Cache field should be set to Optional after first access");
    assertFalse(fieldCache.isPresent(), "Cache Optional should be empty when disabled");

    operations.close();
  }

  /**
   * Test that closing BaseGVFSOperations without ever accessing the client completes safely.
   *
   * <p>This verifies that the lazy initialization doesn't break cleanup when resources are never
   * used.
   */
  @Test
  public void testCloseWithoutClientAccess() throws Exception {
    Configuration conf = createTestConfiguration();
    TestableGVFSOperations operations = new TestableGVFSOperations(conf);

    // Close without ever accessing the client
    operations.close();

    // Verify client is still null
    Field clientField = BaseGVFSOperations.class.getDeclaredField("gravitinoClient");
    clientField.setAccessible(true);
    GravitinoClient client = (GravitinoClient) clientField.get(operations);
    assertNull(client, "Client should remain null if never accessed");
  }

  /**
   * Test that closing BaseGVFSOperations without ever accessing the cache completes safely.
   *
   * <p>This verifies lazy initialization doesn't break cleanup for the cache.
   */
  @Test
  public void testCloseWithoutCacheAccess() throws Exception {
    Configuration conf = createTestConfiguration();
    conf.setBoolean(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_METADATA_CACHE_ENABLE, true);

    TestableGVFSOperations operations = new TestableGVFSOperations(conf);

    // Close without ever accessing the cache
    operations.close();

    // Verify cache is still null
    Field cacheField = BaseGVFSOperations.class.getDeclaredField("filesetMetadataCache");
    cacheField.setAccessible(true);
    Optional<?> cache = (Optional<?>) cacheField.get(operations);
    assertNull(cache, "Cache should remain null if never accessed");
  }
}
