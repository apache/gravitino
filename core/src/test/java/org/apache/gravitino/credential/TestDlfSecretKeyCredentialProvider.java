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
package org.apache.gravitino.credential;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDlfSecretKeyCredentialProvider {

  @Test
  void testDlfSecretKeyCredentialProvider() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID,
            "dlf-ak",
            DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_SECRET,
            "dlf-sk",
            DlfSecretKeyCredential.GRAVITINO_DLF_SECURITY_TOKEN,
            "tok");
    CredentialProvider provider =
        CredentialProviderFactory.create(
            DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE, catalogProperties);
    Optional<Credential> credential =
        provider.getCredentialOptional(new CatalogCredentialContext("u"));
    Assertions.assertTrue(credential.isPresent());
    DlfSecretKeyCredential dlf = (DlfSecretKeyCredential) credential.get();
    Assertions.assertEquals("dlf-ak", dlf.accessKeyId());
    Assertions.assertEquals("dlf-sk", dlf.accessKeySecret());
    Assertions.assertEquals("tok", dlf.securityToken());
    Assertions.assertEquals("dlf-ak", dlf.credentialInfo().get("dlf-access-key-id"));
    Assertions.assertEquals("dlf-sk", dlf.credentialInfo().get("dlf-access-key-secret"));
    Assertions.assertEquals("tok", dlf.credentialInfo().get("dlf-security-token"));
  }

  @Test
  void testWithoutSecurityToken() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID,
            "dlf-ak",
            DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_SECRET,
            "dlf-sk");
    CredentialProvider provider =
        CredentialProviderFactory.create(
            DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE, catalogProperties);
    DlfSecretKeyCredential dlf =
        (DlfSecretKeyCredential)
            provider.getCredentialOptional(new CatalogCredentialContext("u")).get();
    Assertions.assertNull(dlf.securityToken());
    Assertions.assertFalse(dlf.credentialInfo().containsKey("dlf-security-token"));
  }

  @Test
  void testIncompletePairThrows() {
    RuntimeException e =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                CredentialProviderFactory.create(
                    DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE,
                    ImmutableMap.of(DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID, "dlf-ak")));
    Assertions.assertInstanceOf(IllegalArgumentException.class, e.getCause());
  }
}
