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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link HDFSFileSystemProxy} close lifecycle and login configuration. */
public class TestHDFSFileSystemProxy {

  private static final Map<String, String> KERBEROS_CONFIG =
      ImmutableMap.of(
          AuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
          KerberosConfig.PRINCIPAL_KEY, "test@EXAMPLE.COM",
          KerberosConfig.KEY_TAB_URI_KEY, "file:///tmp/test.keytab");

  @Test
  public void testClosingFileSystemStopsKerberosTicketRefresh() throws Exception {
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, authUtils, client);

      FileSystem fs = new HDFSFileSystemProxy(new Path("file:///"), KERBEROS_CONFIG).getProxy();
      verify(client, never()).close();

      // Closing the FileSystem stops the ticket refresh that login started, and only once.
      fs.close();
      fs.close();
      verify(client, times(1)).close();
    }
  }

  @Test
  public void testFailedFileSystemCreationAfterLoginStopsKerberosTicketRefresh() throws Exception {
    KerberosClient client = mockLoggedInClient();
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, authUtils, client);

      // The login succeeds, then FileSystem.newInstance fails on an unknown scheme.
      assertThrows(
          GravitinoRuntimeException.class,
          () -> new HDFSFileSystemProxy(new Path("nosuchfs:///"), KERBEROS_CONFIG));
      verify(client, times(1)).close();
    }
  }

  @Test
  public void testProxyLogsInWithTheGivenConfiguration() throws Exception {
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
      stubKerberosLogin(clients, authUtils, client);
      HDFSFileSystemProxy proxy = new HDFSFileSystemProxy();
      proxy.initFileSystem(new Path("file:///"), config, hadoopConf);
      proxy.getProxy().close();

      clients.verify(() -> KerberosClient.builder(eq("svc-a@EXAMPLE.COM"), eq(hadoopConf)));
    }
  }

  @Test
  public void testProxyLogsInWithKerberosWhateverTheGivenConfigurationSays() throws Exception {
    // Not built by hadoopConfiguration, so Hadoop's own setting is still simple.
    Configuration given = new Configuration();
    assertEquals("simple", given.get(HADOOP_SECURITY_AUTHENTICATION));
    KerberosClient client = mock(KerberosClient.class);
    AtomicReference<String> atLogin = new AtomicReference<>();
    when(client.login(anyString()))
        .thenAnswer(
            invocation -> {
              atLogin.set(given.get(HADOOP_SECURITY_AUTHENTICATION));
              return UserGroupInformation.createRemoteUser("test");
            });
    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      stubKerberosLogin(clients, authUtils, client);
      HDFSFileSystemProxy proxy = new HDFSFileSystemProxy();
      proxy.initFileSystem(new Path("file:///"), KERBEROS_CONFIG, given);
      proxy.getProxy().close();

      clients.verify(() -> KerberosClient.builder(anyString(), eq(given)));
      assertEquals("kerberos", atLogin.get());
    }
  }

  @Test
  public void testLoginSettingsHoldWhatTheProxyLogsInWith() throws Exception {
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
      KerberosClient.Builder builder = stubKerberosLogin(clients, authUtils, client);
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
              KerberosAuthUtils.saveKeytabFromUri(
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
  public void testHadoopConfigurationKeepsTheFlagsOfTheProxy() {
    Configuration conf = HDFSFileSystemProxy.hadoopConfiguration(ImmutableMap.of());
    // A Kerberos client may still reach a cluster with simple authentication, as before.
    assertTrue(conf.getBoolean(HDFSFileSystemProvider.IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED, false));
    assertTrue(conf.getBoolean(Constants.FS_DISABLE_CACHE, false));
  }

  private static KerberosClient mockLoggedInClient() throws Exception {
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString())).thenReturn(UserGroupInformation.createRemoteUser("test"));
    return client;
  }

  private static KerberosClient.Builder stubKerberosLogin(
      MockedStatic<KerberosClient> clients,
      MockedStatic<KerberosAuthUtils> authUtils,
      KerberosClient client) {
    KerberosClient.Builder builder = mock(KerberosClient.Builder.class, Mockito.RETURNS_SELF);
    when(builder.build()).thenReturn(client);
    clients.when(() -> KerberosClient.builder(anyString(), any())).thenReturn(builder);
    authUtils
        .when(
            () ->
                KerberosAuthUtils.saveKeytabFromUri(
                    anyString(), any(), anyInt(), anyBoolean(), any()))
        .thenReturn(new File("test.keytab"));
    return builder;
  }
}
