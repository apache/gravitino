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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.secret.SupportsSecrets;
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

  private static final class TestOps extends BaseGVFSOperations {
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
