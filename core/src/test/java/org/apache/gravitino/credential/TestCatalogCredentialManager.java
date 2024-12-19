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
import org.apache.gravitino.exceptions.NoSuchCredentialException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class TestCatalogCredentialManager {

  @Test
  void testCatalogCredentialManager() {
    CatalogCredentialManager catalogCredentialManager = new CatalogCredentialManager();
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            CredentialConstants.CREDENTIAL_PROVIDERS,
            DummyCredentialProvider.CREDENTIAL_TYPE
                + ","
                + Dummy2CredentialProvider.CREDENTIAL_TYPE);
    catalogCredentialManager.initialize(catalogProperties);
    PathBasedCredentialContext context =
        new PathBasedCredentialContext("test", ImmutableSet.of("location"), ImmutableSet.of());
    Credential credential =
        catalogCredentialManager.getCredential(DummyCredentialProvider.CREDENTIAL_TYPE, context);
    Assertions.assertEquals(DummyCredentialProvider.CREDENTIAL_TYPE, credential.credentialType());
    credential =
        catalogCredentialManager.getCredential(Dummy2CredentialProvider.CREDENTIAL_TYPE, context);
    Assertions.assertEquals(Dummy2CredentialProvider.CREDENTIAL_TYPE, credential.credentialType());
    Assertions.assertThrowsExactly(
        NoSuchCredentialException.class,
        () -> catalogCredentialManager.getCredential("not exists", context));
  }
}
