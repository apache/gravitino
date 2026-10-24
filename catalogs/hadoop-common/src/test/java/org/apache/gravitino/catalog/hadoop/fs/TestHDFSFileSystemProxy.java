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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.kerberos.KerberosConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link HDFSFileSystemProxy} close lifecycle. */
public class TestHDFSFileSystemProxy {

  @Test
  public void testClosingFileSystemStopsKerberosTicketRefresh() throws Exception {
    KerberosClient client = mock(KerberosClient.class);
    when(client.login(anyString())).thenReturn(UserGroupInformation.createRemoteUser("test"));
    KerberosClient.Builder builder = mock(KerberosClient.Builder.class, Mockito.RETURNS_SELF);
    when(builder.build()).thenReturn(client);
    Map<String, String> config =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
            KerberosConfig.PRINCIPAL_KEY, "test@EXAMPLE.COM",
            KerberosConfig.KEY_TAB_URI_KEY, "file:///tmp/test.keytab");

    try (MockedStatic<KerberosClient> clients = mockStatic(KerberosClient.class);
        MockedStatic<KerberosAuthUtils> authUtils = mockStatic(KerberosAuthUtils.class)) {
      clients.when(() -> KerberosClient.builder(anyString(), any())).thenReturn(builder);
      authUtils
          .when(
              () ->
                  KerberosAuthUtils.saveKeytabFromUri(
                      anyString(), any(), anyInt(), anyBoolean(), any()))
          .thenReturn(new File("test.keytab"));

      FileSystem fs = new HDFSFileSystemProxy(new Path("file:///"), config).getProxy();
      verify(client, never()).close();

      // Closing the FileSystem stops the ticket refresh that login started, and only once.
      fs.close();
      fs.close();
      verify(client, times(1)).close();
    }
  }
}
