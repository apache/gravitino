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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.KeytabNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/** Tests for {@link HDFSFileSystemProxy}. */
public class TestHDFSFileSystemProxy {

  private static final Map<String, String> KERBEROS_CONFIG =
      ImmutableMap.of(
          AuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
          KerberosConfig.PRINCIPAL_KEY, "test@EXAMPLE.COM",
          KerberosConfig.KEY_TAB_URI_KEY, "file:///tmp/test.keytab");

  // The keytab files are fetched into this directory instead of one under the working directory,
  // whose locking and cleanup KeytabNames would start.
  @TempDir File keytabRoot;

  private MockedStatic<KeytabNames> keytabNames;

  @BeforeEach
  void fetchKeytabsIntoTheTestDirectory() {
    // Stubbing with when() would call the real method once.
    keytabNames =
        mockStatic(
            KeytabNames.class,
            invocation ->
                "directory".equals(invocation.getMethod().getName())
                    ? new File(keytabRoot, "gravitino-test")
                    : invocation.callRealMethod());
  }

  // Running the shutdown hook hands over every later keytab file. No other test may see that.
  @AfterEach
  void resetKeytabHandOver() {
    try {
      if (keytabNames != null) {
        keytabNames.close();
      }
    } finally {
      HDFSFileSystemProxy.resetKeytabFilesHandedOver();
    }
  }

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
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);

      // The login succeeds, then FileSystem.newInstance fails on an unknown scheme.
      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("nosuchfs:///"), KERBEROS_CONFIG));
      verify(client, times(1)).close();
      assertFalse(keytabs.get(0).exists());
      // The failed proxy freed its name.
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      assertEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
    }
  }

  @Test
  public void testFileSystemCreationErrorAfterLoginStopsKerberosTicketRefresh(
      @TempDir File keytabDir) throws Exception {
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .putAll(KERBEROS_CONFIG)
            .put("fs.errorfs.impl", ErrorOnInitializeFileSystem.class.getName())
            .build();
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);

      assertThrows(
          NoClassDefFoundError.class,
          () -> new HDFSFileSystemProxy(new Path("errorfs:///"), config));
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
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
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
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
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
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
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
  public void testKeytabFileRecordedAfterTheCleanerRanIsDeletedOnExit(@TempDir File keytabDir)
      throws Exception {
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
      FileSystem first = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      try {
        HDFSFileSystemProxy.keytabCleaner().run();
        // As if another shutdown hook opened a Kerberos fileset after the cleaner ran.
        try (FileSystem second =
            new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy()) {
          assertTrue(isDeletedOnExit(keytabs.get(1)));
        }
      } finally {
        first.close();
      }
    }
  }

  @Test
  public void testKeytabFilesAreDeletedOnExitWhenTheCleanerCannotBeAdded(@TempDir File keytabDir)
      throws Exception {
    // As if the JVM began to shut down before the first Kerberos login of this class loader could
    // add the cleaner.
    Runtime runtime = spy(Runtime.getRuntime());
    Mockito.doThrow(new IllegalStateException("Shutdown in progress"))
        .when(runtime)
        .addShutdownHook(
            argThat(hook -> hook != null && "gravitino-keytab-cleaner".equals(hook.getName())));
    AtomicReference<Thread> cleaner = keytabCleanerReference();
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
      // Its keytab is in the set when adding the cleaner fails, like that of a concurrent login
      // that lost the race to add it, so only a hand-over of the set registers it.
      try (FileSystem open =
          new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy()) {
        assertFalse(isDeletedOnExit(keytabs.get(0)));
        Thread registered = cleaner.getAndSet(null);
        try (MockedStatic<Runtime> runtimes =
            mockStatic(Runtime.class, Mockito.CALLS_REAL_METHODS)) {
          runtimes.when(Runtime::getRuntime).thenReturn(runtime);
          try (FileSystem first =
                  new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
              FileSystem second =
                  new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy()) {
            assertTrue(isDeletedOnExit(keytabs.get(0)));
            assertTrue(isDeletedOnExit(keytabs.get(1)));
            assertTrue(isDeletedOnExit(keytabs.get(2)));
          }
        } finally {
          cleaner.set(registered);
        }
      }
    }
  }

  @Test
  public void testLoginsSucceedWhenTheCleanerCannotBeRegistered(@TempDir File keytabDir)
      throws Exception {
    // As under a security manager that denies adding shutdown hooks.
    Runtime runtime = spy(Runtime.getRuntime());
    Mockito.doThrow(new SecurityException("shutdownHooks"))
        .when(runtime)
        .addShutdownHook(
            argThat(hook -> hook != null && "gravitino-keytab-cleaner".equals(hook.getName())));
    AtomicReference<Thread> cleaner = keytabCleanerReference();
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    Thread registered = cleaner.getAndSet(null);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class);
        MockedStatic<Runtime> runtimes = mockStatic(Runtime.class, Mockito.CALLS_REAL_METHODS)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
      runtimes.when(Runtime::getRuntime).thenReturn(runtime);
      try (FileSystem first =
              new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
          FileSystem second =
              new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy()) {
        assertTrue(keytabs.get(0).exists());
        assertTrue(keytabs.get(1).exists());
        // The files are left to the next JVM, not handed to File.deleteOnExit one by one.
        assertFalse(isDeletedOnExit(keytabs.get(0)));
        assertFalse(isDeletedOnExit(keytabs.get(1)));
      }
      // Closing still deletes them.
      assertFalse(keytabs.get(0).exists());
      assertFalse(keytabs.get(1).exists());
      // Hadoop may add a hook of its own while the first file system is created.
      verify(runtime, times(1))
          .addShutdownHook(
              argThat(hook -> hook != null && "gravitino-keytab-cleaner".equals(hook.getName())));
    } finally {
      cleaner.set(registered);
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
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, lockedDir);
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
      // Its name stays taken while the file may still be there.
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      assertNotEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
    }
  }

  @Test
  public void testFailedLoginDeletesKeytabFile(@TempDir File keytabDir) throws Exception {
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString()))
        .thenAnswer(
            invocation -> {
              keytabs.add(new File(invocation.<String>getArgument(0)));
              throw new IOException("login failed");
            });
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);

      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG));
      assertFalse(keytabs.get(0).exists());
      // The failed proxy freed its name.
      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG));
      assertEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
    }
  }

  @Test
  public void testKeytabFileNamesAreReusedOnceDeleted(@TempDir File keytabDir) throws Exception {
    // The JDK keeps every keytab path it has read until the JVM exits.
    Map<String, String> config =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY,
            "kerberos",
            KerberosConfig.PRINCIPAL_KEY,
            "test@EXAMPLE.COM",
            KerberosConfig.KEY_TAB_URI_KEY,
            "file:///tmp/test.keytab",
            "gravitino.identifier",
            "fileset-reused");
    Map<String, String> otherKeytab =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY,
            "kerberos",
            KerberosConfig.PRINCIPAL_KEY,
            "test@EXAMPLE.COM",
            KerberosConfig.KEY_TAB_URI_KEY,
            "file:///tmp/other.keytab",
            "gravitino.identifier",
            "fileset-reused");
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);

      new HDFSFileSystemProxy(new Path("file:///"), config).getProxy().close();
      new HDFSFileSystemProxy(new Path("file:///"), config).getProxy().close();
      assertEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
      // A file with other keys never takes that name, so the JDK never reads it as this one. The
      // stub writes the URI into the file.
      new HDFSFileSystemProxy(new Path("file:///"), otherKeytab).getProxy().close();
      assertNotEquals(keytabs.get(0).getName(), keytabs.get(2).getName());
    }
  }

  @Test
  public void testLoginReadsTheKeysOfAReplacedKeytab(
      @TempDir File keytabDir, @TempDir File sourceDir) throws Exception {
    File source = new File(sourceDir, "test.keytab");
    writeKeytab(source, 1);
    FileTime modified = Files.getLastModifiedTime(source.toPath());
    // The key versions each login reads from its keytab file with the JDK.
    List<String> versions = new ArrayList<>();
    List<Boolean> links = new ArrayList<>();
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString()))
        .thenAnswer(
            invocation -> {
              File keytab = new File(invocation.<String>getArgument(0));
              keytabs.add(keytab);
              versions.add(keyVersions(keytab));
              // Renewals of an open proxy still follow a local keytab.
              links.add(Files.isSymbolicLink(keytab.toPath()));
              return UserGroupInformation.createRemoteUser("test");
            });
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      // A file URI is fetched as a symbolic link to the file.
      authUtils
          .when(
              () ->
                  KerberosAuthUtils.fetchKeytabFromUri(
                      anyString(), any(), anyInt(), anyBoolean(), any()))
          .thenAnswer(
              invocation -> {
                File keytab = new File(keytabDir, invocation.<File>getArgument(1).getName());
                Files.createSymbolicLink(keytab.toPath(), source.toPath());
                return keytab;
              });

      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      // The JDK returns the keys it cached for a path while the file there keeps its modification
      // time, and also when the file there is not a valid keytab.
      writeKeytab(source, 2);
      Files.setLastModifiedTime(source.toPath(), modified);
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      Files.write(source.toPath(), new byte[] {1, 2, 3});
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      assertEquals(Arrays.asList("1", "2", ""), versions);

      // The same keys with the same modification time get the same name again.
      writeKeytab(source, 1);
      Files.setLastModifiedTime(source.toPath(), modified);
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      assertEquals(keytabs.get(0).getName(), keytabs.get(3).getName());
      assertEquals("1", versions.get(3));
      // The name tells nothing about the content of the file, which may be any file the server
      // can read, nor whether two keytab URIs hold the same bytes.
      byte[] content = Files.readAllBytes(source.toPath());
      byte[] uri =
          KERBEROS_CONFIG.get(KerberosConfig.KEY_TAB_URI_KEY).getBytes(StandardCharsets.UTF_8);
      for (String sha256 :
          new String[] {
            Hashing.sha256().hashBytes(content).toString(),
            Hashing.sha256()
                .newHasher()
                .putInt(uri.length)
                .putBytes(uri)
                .putBytes(content)
                .hash()
                .toString()
          }) {
        assertFalse(keytabs.get(3).getName().contains(sha256), keytabs.get(3).getName());
      }
      Map<String, String> otherUri =
          ImmutableMap.<String, String>builder()
              .putAll(KERBEROS_CONFIG)
              .put(KerberosConfig.KEY_TAB_URI_KEY, "file:///tmp/other.keytab")
              .buildKeepingLast();
      new HDFSFileSystemProxy(new Path("file:///"), otherUri).getProxy().close();
      assertNotEquals(keytabs.get(3).getName(), keytabs.get(4).getName());
      assertEquals(Arrays.asList(true, true, true, true, true), links);
      assertArrayEquals(new String[0], keytabDir.list());
    }
  }

  @Test
  public void testKeytabFileNamesAreReusedAfterACatalogReload(@TempDir File keytabDir)
      throws Exception {
    // A catalog class loader loads this class again after the last catalog that used it is
    // closed, while the JDK keeps the keys of every path it read. The names come from one table
    // that all of them share.
    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .putAll(KERBEROS_CONFIG)
            .put("gravitino.identifier", "fileset-reloaded")
            .build();
    String keytabUri = config.get(KerberosConfig.KEY_TAB_URI_KEY);
    String prefix =
        KeytabNames.prefix(
            "fileset-reloaded", keytabUri, keytabUri.getBytes(StandardCharsets.UTF_8), null);
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);
      // As if a proxy of an earlier class loader still held the lowest free name.
      int held = KeytabNames.reserve(prefix);
      try {
        new HDFSFileSystemProxy(new Path("file:///"), config).getProxy().close();
        assertEquals("gravitino-" + prefix + "-" + (held + 1), keytabs.get(0).getName());
      } finally {
        KeytabNames.release(prefix, held);
      }
      new HDFSFileSystemProxy(new Path("file:///"), config).getProxy().close();
      assertEquals("gravitino-" + prefix + "-" + held, keytabs.get(1).getName());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNameOfALinkReplacedWhileItsProxyWasOpenIsNotReused(
      boolean deletedOnClose, @TempDir File keytabDir, @TempDir File sourceDir) throws Exception {
    File source = new File(sourceDir, "test.keytab");
    writeKeytab(source, 1);
    List<String> versions = new ArrayList<>();
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString()))
        .thenAnswer(
            invocation -> {
              File keytab = new File(invocation.<String>getArgument(0));
              keytabs.add(keytab);
              versions.add(keyVersions(keytab));
              return UserGroupInformation.createRemoteUser("test");
            });
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      authUtils
          .when(
              () ->
                  KerberosAuthUtils.fetchKeytabFromUri(
                      anyString(), any(), anyInt(), anyBoolean(), any()))
          .thenAnswer(
              invocation -> {
                File keytab = new File(keytabDir, invocation.<File>getArgument(1).getName());
                Files.createSymbolicLink(keytab.toPath(), source.toPath());
                return keytab;
              });

      FileSystem open = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      // The keytab is rotated while the proxy is open, and a renewal reads the new keys through the
      // link, so the JDK keeps them for this path.
      FileTime rotated =
          FileTime.fromMillis(Files.getLastModifiedTime(source.toPath()).toMillis() + 2_000);
      writeKeytab(source, 2);
      Files.setLastModifiedTime(source.toPath(), rotated);
      assertEquals("2", keyVersions(keytabs.get(0)));
      if (deletedOnClose) {
        // Closing reads nothing through the link, so the file may be missing then.
        Files.delete(source.toPath());
        open.close();
      }
      // Then the old keys come back with the same modification time.
      writeKeytab(source, 1);
      Files.setLastModifiedTime(source.toPath(), rotated);
      if (!deletedOnClose) {
        open.close();
      }

      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      // The next login reads the keys of the file, not those the JDK kept for the old name.
      assertEquals(Arrays.asList("1", "1"), versions);
      assertNotEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
      assertArrayEquals(new String[0], keytabDir.list());
    }
  }

  @Test
  public void testClosingDoesNotReadTheTargetOfALink(
      @TempDir File keytabDir, @TempDir File sourceDir) throws Exception {
    File source = new File(sourceDir, "test.keytab");
    writeKeytab(source, 1);
    FileTime modified = Files.getLastModifiedTime(source.toPath());
    List<String> versions = new ArrayList<>();
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString()))
        .thenAnswer(
            invocation -> {
              File keytab = new File(invocation.<String>getArgument(0));
              keytabs.add(keytab);
              versions.add(keyVersions(keytab));
              return UserGroupInformation.createRemoteUser("test");
            });
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabLink(authUtils, keytabDir, source);

      FileSystem open = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      // The source cannot be read while the proxy closes, as on a mount that does not answer. The
      // mock sees the calls of Files on this thread, not those of java.io.File.
      Files.delete(source.toPath());
      File link = keytabs.get(0);
      List<String> followed = new ArrayList<>();
      try (MockedStatic<Files> files =
          mockStatic(
              Files.class,
              invocation -> {
                if (followsLink(invocation, link)) {
                  followed.add(invocation.getMethod().getName());
                }
                return invocation.callRealMethod();
              })) {
        open.close();
      }
      assertEquals(new ArrayList<>(), followed);

      // The same keys with the same modification time get the same name again.
      writeKeytab(source, 1);
      Files.setLastModifiedTime(source.toPath(), modified);
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      assertEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
      assertEquals(Arrays.asList("1", "1"), versions);
      assertArrayEquals(new String[0], keytabDir.list());
    }
  }

  @Test
  public void testOversizedKeytabIsNotReadForItsName(
      @TempDir File keytabDir, @TempDir File sourceDir) throws Exception {
    // Larger than any array, so reading all of it fails. Most file systems keep it sparse.
    File source = new File(sourceDir, "huge.keytab");
    try (RandomAccessFile file = new RandomAccessFile(source, "rw")) {
      file.setLength(Integer.MAX_VALUE + 1L);
    }
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      authUtils
          .when(
              () ->
                  KerberosAuthUtils.fetchKeytabFromUri(
                      anyString(), any(), anyInt(), anyBoolean(), any()))
          .thenAnswer(
              invocation -> {
                File keytab = new File(keytabDir, invocation.<File>getArgument(1).getName());
                Files.createSymbolicLink(keytab.toPath(), source.toPath());
                return keytab;
              });

      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy().close();
      // Without reading all of it, the same content cannot be told apart, so no name is reused.
      assertNotEquals(keytabs.get(0).getName(), keytabs.get(1).getName());
      assertArrayEquals(new String[0], keytabDir.list());
    }
  }

  @Test
  public void testFailedFetchDeletesWhatItWrote() throws Exception {
    KerberosClient client = mockLoggedInClient();
    AtomicReference<File> destination = new AtomicReference<>();
    AtomicBoolean written = new AtomicBoolean();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      // Like a download that writes part of the file in place and then fails.
      authUtils
          .when(
              () ->
                  KerberosAuthUtils.fetchKeytabFromUri(
                      anyString(), any(), anyInt(), anyBoolean(), any()))
          .thenAnswer(
              invocation -> {
                File file = invocation.getArgument(1);
                destination.set(file);
                Files.createDirectories(file.getAbsoluteFile().getParentFile().toPath());
                Files.write(file.toPath(), new byte[] {1});
                written.set(true);
                throw new IOException("Connection reset");
              });
      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG));
      assertTrue(written.get());
      assertFalse(destination.get().exists());
      assertFalse(HDFSFileSystemProxy.isLiveKeytabFile(destination.get()));
      assertTrue(destination.get().toPath().startsWith(keytabRoot.toPath()));
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
    List<File> keytabs = new ArrayList<>();
    KerberosClient client = mockLoggedInClient(keytabs);
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, client);
      stubKeytabFetch(authUtils, keytabDir);

      FileSystem first = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      FileSystem second = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      assertNotEquals(keytabs.get(0), keytabs.get(1));

      // Closing a file system deletes its own keytab file only.
      first.close();
      assertFalse(keytabs.get(0).exists());
      assertTrue(keytabs.get(1).exists());
      second.close();
      assertFalse(keytabs.get(1).exists());

      // A name freed by a closed proxy is reused, never one an open proxy still holds.
      FileSystem a = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      FileSystem b = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      a.close();
      FileSystem c = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      FileSystem d = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      try {
        String nameOfB = keytabs.get(3).getName();
        assertEquals(keytabs.get(2).getName(), keytabs.get(4).getName());
        assertNotEquals(nameOfB, keytabs.get(4).getName());
        assertNotEquals(nameOfB, keytabs.get(5).getName());
        assertNotEquals(keytabs.get(4).getName(), keytabs.get(5).getName());
        // Closing b frees its own name, not another one.
        b.close();
        try (FileSystem e = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy()) {
          assertEquals(nameOfB, keytabs.get(6).getName());
        }
      } finally {
        b.close();
        c.close();
        d.close();
      }
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

  @Test
  public void testLoginSettingsExpandVariablesWhereverTheirPropertiesComeFrom(
      @TempDir File resourceDir) throws Exception {
    assertFalse(
        HDFSFileSystemProxy.loginSettingsExpandVariables(
            HDFSFileSystemProxy.hadoopConfiguration(KERBEROS_CONFIG)));
    // Each Hadoop property the settings come from, set to a variable that expands to kerberos.
    for (String key :
        new String[] {
          HADOOP_SECURITY_AUTHENTICATION,
          Constants.HADOOP_SECURITY_PRINCIPAL,
          Constants.HADOOP_SECURITY_KEYTAB,
          Constants.HADOOP_KRB5_CONF
        }) {
      Map<String, String> config =
          ImmutableMap.<String, String>builder()
              .putAll(KERBEROS_CONFIG)
              .put(FileSystemProvider.GRAVITINO_BYPASS + "auth", "kerberos")
              .put(FileSystemProvider.GRAVITINO_BYPASS + key, "${auth}")
              .build();
      assertTrue(
          HDFSFileSystemProxy.loginSettingsExpandVariables(
              HDFSFileSystemProxy.hadoopConfiguration(config)),
          key);
    }

    // A variable that expands to another spelling of kerberos is still seen, though the value is
    // normalized for the login.
    Map<String, String> upperCase =
        ImmutableMap.<String, String>builder()
            .putAll(KERBEROS_CONFIG)
            .put(FileSystemProvider.GRAVITINO_BYPASS + "auth", "KERBEROS")
            .put(FileSystemProvider.GRAVITINO_BYPASS + HADOOP_SECURITY_AUTHENTICATION, "${auth}")
            .build();
    Configuration normalized = HDFSFileSystemProxy.hadoopConfiguration(upperCase);
    assertTrue(HDFSFileSystemProxy.loginSettingsExpandVariables(normalized));
    assertEquals("kerberos", normalized.get(HADOOP_SECURITY_AUTHENTICATION));

    // A variable in a config.resources file counts too.
    File resource = new File(resourceDir, "login-site.xml");
    Files.write(
        resource.toPath(),
        ("<configuration><property><name>"
                + Constants.HADOOP_SECURITY_PRINCIPAL
                + "</name><value>svc@${realm}</value></property></configuration>")
            .getBytes(StandardCharsets.UTF_8));
    Map<String, String> fromResource =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY,
            "kerberos",
            Constants.CONFIG_RESOURCES,
            resource.getAbsolutePath());
    assertTrue(
        HDFSFileSystemProxy.loginSettingsExpandVariables(
            HDFSFileSystemProxy.hadoopConfiguration(fromResource)));
  }

  // A keytab with one AES-128 key of the given version for test@EXAMPLE.COM.
  private static void writeKeytab(File file, int version) throws IOException {
    ByteArrayOutputStream entry = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(entry);
    out.writeShort(1);
    for (String part : new String[] {"EXAMPLE.COM", "test"}) {
      byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
      out.writeShort(bytes.length);
      out.write(bytes);
    }
    out.writeInt(1); // KRB5_NT_PRINCIPAL
    out.writeInt(0); // timestamp
    out.writeByte(version);
    out.writeShort(17); // aes128-cts-hmac-sha1-96
    out.writeShort(16);
    out.write(new byte[16]);
    out.writeInt(version);
    ByteArrayOutputStream keytab = new ByteArrayOutputStream();
    DataOutputStream header = new DataOutputStream(keytab);
    header.writeShort(0x0502);
    header.writeInt(entry.size());
    header.write(entry.toByteArray());
    Files.write(file.toPath(), keytab.toByteArray());
  }

  private static String keyVersions(File keytab) {
    return Arrays.stream(
            KeyTab.getInstance(keytab).getKeys(new KerberosPrincipal("test@EXAMPLE.COM")))
        .map(key -> String.valueOf(key.getVersionNumber()))
        .collect(Collectors.joining(","));
  }

  private static KerberosClient mockLoggedInClient() throws Exception {
    return mockLoggedInClient(new ArrayList<>());
  }

  // Records the keytab file of each login.
  private static KerberosClient mockLoggedInClient(List<File> keytabs) throws Exception {
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString()))
        .thenAnswer(
            invocation -> {
              keytabs.add(new File(invocation.<String>getArgument(0)));
              return UserGroupInformation.createRemoteUser("test");
            });
    return client;
  }

  private static KerberosClient.Builder stubKerberosLogin(
      MockedStatic<KerberosClient> clients, KerberosClient client) {
    KerberosClient.Builder builder = mock(KerberosClient.Builder.class, Mockito.RETURNS_SELF);
    when(builder.build()).thenReturn(client);
    clients.when(() -> KerberosClient.builder(anyString(), any())).thenReturn(builder);
    return builder;
  }

  @SuppressWarnings("unchecked")
  private static AtomicReference<Thread> keytabCleanerReference() throws Exception {
    Field field = HDFSFileSystemProxy.class.getDeclaredField("KEYTAB_CLEANER");
    field.setAccessible(true);
    return (AtomicReference<Thread>) field.get(null);
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

  // Fetches each keytab as a link to the source, as for a file URI.
  private static void stubKeytabLink(
      MockedStatic<KerberosAuthUtils> authUtils, File keytabDir, File source) {
    authUtils
        .when(
            () ->
                KerberosAuthUtils.fetchKeytabFromUri(
                    anyString(), any(), anyInt(), anyBoolean(), any()))
        .thenAnswer(
            invocation -> {
              File keytab = new File(keytabDir, invocation.<File>getArgument(1).getName());
              Files.createSymbolicLink(keytab.toPath(), source.toPath());
              return keytab;
            });
  }

  // Whether a call of Files on the file follows it when it is a link. Deleting a link does not,
  // nor does looking up the file system provider of its path.
  private static boolean followsLink(InvocationOnMock invocation, File file) {
    String method = invocation.getMethod().getName();
    if (method.equals("delete") || method.equals("deleteIfExists") || method.equals("provider")) {
      return false;
    }
    List<Object> arguments = new ArrayList<>();
    for (Object argument : invocation.getArguments()) {
      if (argument instanceof Object[]) {
        arguments.addAll(Arrays.asList((Object[]) argument));
      } else {
        arguments.add(argument);
      }
    }
    return arguments.contains(file.toPath()) && !arguments.contains(LinkOption.NOFOLLOW_LINKS);
  }

  // Writes each fetched keytab into the directory, with its URI as its content.
  private static void stubKeytabFetch(MockedStatic<KerberosAuthUtils> authUtils, File keytabDir) {
    authUtils
        .when(
            () ->
                KerberosAuthUtils.fetchKeytabFromUri(
                    anyString(), any(), anyInt(), anyBoolean(), any()))
        .thenAnswer(
            invocation -> {
              File keytab = new File(keytabDir, invocation.<File>getArgument(1).getName());
              Files.write(
                  keytab.toPath(),
                  invocation.<String>getArgument(0).getBytes(StandardCharsets.UTF_8));
              return keytab;
            });
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

  /**
   * A file system whose initialization fails with an error, for example from a class that a
   * released catalog class loader can no longer load.
   */
  public static final class ErrorOnInitializeFileSystem extends RawLocalFileSystem {
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      throw new NoClassDefFoundError("org/example/Missing");
    }
  }
}
