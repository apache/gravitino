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
package org.apache.gravitino.iceberg.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.ClientPool;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestClosableHiveCatalog {

  private UserGroupInformation loginUser;
  private IMetaStoreClient metaStoreClient;

  @BeforeEach
  void setUp() throws Exception {
    loginUser = UserGroupInformation.createRemoteUser("gravitino");
    metaStoreClient = Mockito.mock(IMetaStoreClient.class);
    Mockito.when(metaStoreClient.getDelegationToken(anyString(), anyString()))
        .thenReturn(new Token<DelegationTokenIdentifier>().encodeToUrlString());
  }

  @Test
  void testNoTokenWithoutImpersonation() throws Throwable {
    ClosableHiveCatalog catalog = newCatalog(false);

    String user = runAsAlice(catalog);

    Assertions.assertEquals("gravitino", user);
    Mockito.verify(metaStoreClient, Mockito.never()).getDelegationToken(anyString(), anyString());
    Assertions.assertEquals(0, loginUser.getCredentials().numberOfTokens());
  }

  @Test
  void testTokenOnProxyUserWithImpersonation() throws Throwable {
    ClosableHiveCatalog catalog = newCatalog(true);

    String user = runAsAlice(catalog);

    Assertions.assertEquals("alice@EXAMPLE.COM", user);
    Mockito.verify(metaStoreClient).getDelegationToken("alice@EXAMPLE.COM", "gravitino");
    Assertions.assertEquals(0, loginUser.getCredentials().numberOfTokens());
  }

  private String runAsAlice(ClosableHiveCatalog catalog) throws Exception {
    return PrincipalUtils.doAs(
        new UserPrincipal("alice"),
        () -> {
          try {
            return catalog.doKerberosOperations(
                () -> {
                  UserGroupInformation current = UserGroupInformation.getCurrentUser();
                  if (!current.getUserName().equals(loginUser.getUserName())) {
                    Assertions.assertEquals(1, current.getCredentials().numberOfTokens());
                  }
                  return current.getUserName();
                });
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        });
  }

  @SuppressWarnings("unchecked")
  private ClosableHiveCatalog newCatalog(boolean impersonation) throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(
            AuthenticationConfig.AUTH_TYPE_KEY,
            "kerberos",
            AuthenticationConfig.IMPERSONATION_ENABLE_KEY,
            String.valueOf(impersonation));
    ClientPool<IMetaStoreClient, TException> clientPool = Mockito.mock(ClientPool.class);
    Mockito.when(clientPool.run(any()))
        .thenAnswer(
            invocation ->
                ((ClientPool.Action<?, IMetaStoreClient, TException>) invocation.getArgument(0))
                    .run(metaStoreClient));
    KerberosClient kerberosClient = Mockito.mock(KerberosClient.class);
    Mockito.when(kerberosClient.getLoginUser()).thenReturn(loginUser);
    Mockito.when(kerberosClient.getRealm()).thenReturn("EXAMPLE.COM");

    ClosableHiveCatalog catalog = new ClosableHiveCatalog();
    FieldUtils.writeField(catalog, "catalogProperties", properties, true);
    FieldUtils.writeField(catalog, "clients", clientPool, true);
    FieldUtils.writeField(catalog, "kerberosClient", kerberosClient, true);
    return catalog;
  }
}
