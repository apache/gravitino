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

package org.apache.gravitino.catalog.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link HDFSFileSystemProxy}. */
public class TestHDFSFileSystemProxy {

  private static final Map<String, String> KERBEROS_CONFIG =
      ImmutableMap.of(
          AuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
          KerberosConfig.PRINCIPAL_KEY, "test@EXAMPLE.COM",
          KerberosConfig.KEY_TAB_URI_KEY, "file:///tmp/test.keytab");

  @Test
  public void testClosingFileSystemStopsKerberosTicketRefresh(@TempDir File keytabDir)
      throws Exception {
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);

      FileSystem fs = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      verify(client, never()).close();

      // Closing the FileSystem stops the ticket refresh that login started, and only once.
      fs.close();
      fs.close();
      verify(client, times(1)).close();
    }
  }

  @Test
  public void testFailedFileSystemCreationAfterLoginStopsKerberosTicketRefresh(
      @TempDir File keytabDir) throws Exception {
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, keytabDir);

      // The login succeeds, then FileSystem.newInstance fails on an unknown scheme.
      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("nosuchfs:///"), KERBEROS_CONFIG));
      verify(client, times(1)).close();
      assertFalse(keytabs.get(0).exists());
    }
  }

  @Test
  public void testFailedWrappingReleasesFileSystemAndLogin(@TempDir File keytabDir)
      throws Exception {
    // CGLIB cannot subclass a final class, so getProxy() fails after the login.
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .putAll(KERBEROS_CONFIG)
            .put("fs.finalfs.impl", FinalFileSystem.class.getName())
            .build();
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, keytabDir);
      HDFSFileSystemProxy proxy = new HDFSFileSystemProxy(new Path("finalfs:///"), config);
      int closedBefore = FinalFileSystem.CLOSED.get();

      assertThrows(IllegalArgumentException.class, proxy::getProxy);
      assertEquals(closedBefore + 1, FinalFileSystem.CLOSED.get());
      verify(client, times(1)).close();
      assertFalse(keytabs.get(0).exists());
    }
  }

  @Test
  public void testProxyLogsInWithTheGivenConfiguration(@TempDir File keytabDir) throws Exception {
    Configuration hadoopConf =
        HDFSFileSystemProxy.hadoopConfiguration(
            ImmutableMap.of(
                AuthenticationConfig.AUTH_TYPE_KEY,
                "kerberos",
                Constants.HADOOP_SECURITY_PRINCIPAL,
                "svc-a@EXAMPLE.COM"));
    // Rebuilding the Hadoop configuration from this map would log in as svc-b.
    Map<String, String> config =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
            Constants.HADOOP_SECURITY_PRINCIPAL, "svc-b@EXAMPLE.COM",
            KerberosConfig.KEY_TAB_URI_KEY, "file:///tmp/test.keytab");
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
      HDFSFileSystemProxy proxy = new HDFSFileSystemProxy();
      proxy.initFileSystem(new Path("file:///"), config, hadoopConf);
      proxy.getProxy().close();

      clients.verify(() -> KerberosClient.builder(eq("svc-a@EXAMPLE.COM"), eq(hadoopConf)));
    }
  }

  @Test
  public void testFailedWrappingReleasesLoginWhenClosingFails(@TempDir File keytabDir)
      throws Exception {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .putAll(KERBEROS_CONFIG)
            .put("fs.failclosefs.impl", FailingCloseFileSystem.class.getName())
            .build();
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, keytabDir);
      HDFSFileSystemProxy proxy = new HDFSFileSystemProxy(new Path("failclosefs:///"), config);

      // The wrapping failure stays the thrown exception, unchanged, and the login is released.
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, proxy::getProxy);
      assertEquals(0, e.getSuppressed().length);
      verify(client, times(1)).close();
      assertFalse(keytabs.get(0).exists());
    }
  }

  @Test
  public void testKeytabFilesOfOpenProxiesAreDeletedOnExit(@TempDir File keytabDir)
      throws Exception {
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, keytabDir);
      FileSystem closed = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      FileSystem open = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      try {
        closed.close();
        // A closed proxy no longer owns its path, even if a file appears there again.
        Files.write(keytabs.get(0).toPath(), new byte[0]);

        // The registered shutdown hook leaves the files of the proxies not closed yet to
        // File.deleteOnExit, which runs after all shutdown hooks.
        Thread cleaner = HDFSFileSystemProxy.keytabCleaner();
        assertTrue(Runtime.getRuntime().removeShutdownHook(cleaner));
        Runtime.getRuntime().addShutdownHook(cleaner);
        cleaner.run();
        assertTrue(keytabs.get(1).exists());
        assertTrue(isDeletedOnExit(keytabs.get(1)));
        assertFalse(isDeletedOnExit(keytabs.get(0)));
      } finally {
        open.close();
      }
    }
  }

  @Test
  public void testKeytabFileThatFailsToDeleteOnCloseIsDeletedOnExit(@TempDir File keytabDir)
      throws Exception {
    File lockedDir = new File(keytabDir, "locked");
    Files.createDirectories(lockedDir.toPath());
    // Deleting a file from a directory without write permission fails, except for root.
    boolean lockable = lockedDir.setWritable(false) && !lockedDir.canWrite();
    lockedDir.setWritable(true);
    assumeTrue(lockable, "a directory without write permission does not stop root");
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, lockedDir);
      FileSystem fs = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      try {
        lockedDir.setWritable(false);
        fs.close();
        verify(client, times(1)).close();
        assertTrue(keytabs.get(0).exists());
      } finally {
        lockedDir.setWritable(true);
      }

      // The failure may be temporary, so the file is still deleted at exit.
      assertTrue(HDFSFileSystemProxy.isLiveKeytabFile(keytabs.get(0)));
      assertTrue(isDeletedOnExit(keytabs.get(0)));
    }
  }

  @Test
  public void testFailedLoginDeletesKeytabFile(@TempDir File keytabDir) throws Exception {
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString())).thenThrow(new IOException("login failed"));
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, keytabDir);

      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG));
      assertFalse(keytabs.get(0).exists());
    }
  }

  @Test
  public void testEachProxyKeepsItsOwnKeytabFile(@TempDir File keytabDir) throws Exception {
    // The proxies of one fileset share its identifier. A shared keytab file would be rewritten by
    // the next proxy while an earlier one still renews its ticket from it.
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .putAll(KERBEROS_CONFIG)
            .put("gravitino.identifier", "fileset-1")
            .build();
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      List<File> keytabs = stubKeytabFetch(authUtils, keytabDir);

      FileSystem first = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      FileSystem second = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      assertNotEquals(keytabs.get(0), keytabs.get(1));

      // Closing a file system deletes its own keytab file only.
      first.close();
      assertFalse(keytabs.get(0).exists());
      assertTrue(keytabs.get(1).exists());
      second.close();
      assertFalse(keytabs.get(1).exists());
    }
  }

  @Test
  public void testLoginSettingsHoldWhatTheProxyLogsInWith(@TempDir File keytabDir)
      throws Exception {
    // The proxy switches Hadoop to Kerberos authentication before it expands the principal.
    Map<String, String> config =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY,
            "kerberos",
            Constants.HADOOP_SECURITY_PRINCIPAL,
            "svc-${" + HADOOP_SECURITY_AUTHENTICATION + "}@EXAMPLE.COM",
            KerberosConfig.KEY_TAB_URI_KEY,
            "file:///tmp/test.keytab",
            FileSystemProvider.GRAVITINO_BYPASS + Constants.HADOOP_KRB5_CONF,
            "/tmp/test-krb5.conf");
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      KerberosClient.Builder builder = stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
      new HDFSFileSystemProxy(new Path("file:///"), config).getProxy().close();

      Map<String, String> settings =
          HDFSFileSystemProxy.loginSettings(
              config, HDFSFileSystemProxy.hadoopConfiguration(config));
      assertEquals("svc-kerberos@EXAMPLE.COM", settings.get(KerberosConfig.PRINCIPAL_KEY));
      assertEquals("/tmp/test-krb5.conf", settings.get(Constants.HADOOP_KRB5_CONF));
      ArgumentCaptor<Configuration> loginConf = ArgumentCaptor.forClass(Configuration.class);
      clients.verify(
          () ->
              KerberosClient.builder(
                  eq(settings.get(KerberosConfig.PRINCIPAL_KEY)), loginConf.capture()));
      authUtils.verify(
          () ->
              KerberosAuthUtils.fetchKeytabFromUri(
                  eq(settings.get(KerberosConfig.KEY_TAB_URI_KEY)),
                  any(),
                  anyInt(),
                  anyBoolean(),
                  any()));
      // The login points the JVM at the krb5.conf stored under this key.
      ArgumentCaptor<String> krb5ConfKey = ArgumentCaptor.forClass(String.class);
      verify(builder).krb5Conf(krb5ConfKey.capture(), any());
      assertEquals(
          settings.get(Constants.HADOOP_KRB5_CONF),
          loginConf.getValue().get(krb5ConfKey.getValue()));
    }
  }

  private static KerberosClient mockLoggedInClient() throws Exception {
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString())).thenReturn(UserGroupInformation.createRemoteUser("test"));
    return client;
  }

  private static KerberosClient.Builder stubKerberosLogin(
      MockedStatic<KerberosClient> clients, KerberosClient client) {
    KerberosClient.Builder builder = mock(KerberosClient.Builder.class, Mockito.RETURNS_SELF);
    when(builder.build()).thenReturn(client);
    clients.when(() -> KerberosClient.builder(anyString(), any())).thenReturn(builder);
    return builder;
  }

  // Whether java.io.DeleteOnExitHook deletes the file at JVM exit. Needs --add-opens
  // java.base/java.io, which the Gradle test task passes.
  private static boolean isDeletedOnExit(File file) throws Exception {
    Class<?> hook = Class.forName("java.io.DeleteOnExitHook");
    Field files = hook.getDeclaredField("files");
    files.setAccessible(true);
    synchronized (hook) {
      return ((Set<?>) files.get(null)).contains(file.getPath());
    }
  }

  // Writes each fetched keytab into the directory and records it.
  private static List<File> stubKeytabFetch(
      MockedStatic<KerberosAuthUtils> authUtils, File keytabDir) {
    List<File> keytabs = new ArrayList<>();
    authUtils
        .when(
            () ->
                KerberosAuthUtils.fetchKeytabFromUri(
                    anyString(), any(), anyInt(), anyBoolean(), any()))
        .thenAnswer(
            invocation -> {
              File keytab = new File(keytabDir, invocation.<File>getArgument(1).getName());
              Files.write(keytab.toPath(), new byte[0]);
              keytabs.add(keytab);
              return keytab;
            });
    return keytabs;
  }

  /** A file system that CGLIB cannot subclass, and whose close fails. */
  public static final class FailingCloseFileSystem extends RawLocalFileSystem {
    @Override
    public void close() throws IOException {
      super.close();
      throw new IllegalStateException("close failed");
    }
  }

  /** A file system that CGLIB cannot subclass, and that counts how often it is closed. */
  public static final class FinalFileSystem extends RawLocalFileSystem {
    static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public void close() throws IOException {
      CLOSED.incrementAndGet();
      super.close();
    }
  }
}
