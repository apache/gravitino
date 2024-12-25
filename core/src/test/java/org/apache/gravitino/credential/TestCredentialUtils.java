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

package org.apache.gravitino.credential;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class TestCredentialUtils {

  @Test
  void testLoadCredentialProviders() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            CredentialConstants.CREDENTIAL_PROVIDERS,
            DummyCredentialProvider.CREDENTIAL_TYPE
                + ","
                + Dummy2CredentialProvider.CREDENTIAL_TYPE);
    Map<String, CredentialProvider> providers =
        CredentialUtils.loadCredentialProviders(catalogProperties);
    Assertions.assertTrue(providers.size() == 2);

    Assertions.assertTrue(providers.containsKey(DummyCredentialProvider.CREDENTIAL_TYPE));
    Assertions.assertTrue(
        DummyCredentialProvider.CREDENTIAL_TYPE.equals(
            providers.get(DummyCredentialProvider.CREDENTIAL_TYPE).credentialType()));
    Assertions.assertTrue(providers.containsKey(Dummy2CredentialProvider.CREDENTIAL_TYPE));
    Assertions.assertTrue(
        Dummy2CredentialProvider.CREDENTIAL_TYPE.equals(
            providers.get(Dummy2CredentialProvider.CREDENTIAL_TYPE).credentialType()));
  }

  @Test
  void testGetCredentialProviders() {
    Map<String, String> filesetProperties = ImmutableMap.of();
    Map<String, String> schemaProperties =
        ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, "a,b");
    Map<String, String> catalogProperties =
        ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, "a,b,c");

    Set<String> credentialProviders =
        CredentialUtils.getCredentialProvidersByOrder(
            () -> filesetProperties, () -> schemaProperties, () -> catalogProperties);
    Assertions.assertEquals(credentialProviders, ImmutableSet.of("a", "b"));
  }
}
