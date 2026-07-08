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
package org.apache.gravitino.iceberg.common.authentication.kerberos;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKerberosCatalogUtils {

  @Test
  void testOpsSimpleAuth() throws Throwable {
    Map<String, String> properties = new HashMap<>();
    properties.put(AuthenticationConfig.AUTH_TYPE_KEY, "simple");

    String result = KerberosCatalogUtils.doKerberosOperations(properties, null, () -> "ok");

    Assertions.assertEquals("ok", result);
  }

  @Test
  void testOpsNoClient() {
    Map<String, String> properties = new HashMap<>();
    properties.put(AuthenticationConfig.AUTH_TYPE_KEY, "kerberos");

    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> KerberosCatalogUtils.doKerberosOperations(properties, null, () -> "ok"));

    Assertions.assertTrue(
        exception.getMessage().contains("Kerberos is configured but KerberosClient is not"));
  }

  @Test
  void testProxyPrincipalRealm() {
    KerberosClient kerberosClient = Mockito.mock(KerberosClient.class);
    Mockito.when(kerberosClient.getRealm()).thenReturn("EXAMPLE.COM");

    String principal = KerberosCatalogUtils.resolveProxyPrincipal(kerberosClient);

    Assertions.assertTrue(principal.endsWith("@EXAMPLE.COM"));
  }
}
