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

public class TestAwsSecretKeyCredentialProvider {

  @Test
  void testAwsSecretKeyCredentialProvider() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            AwsSecretKeyCredential.GRAVITINO_AWS_ACCESS_KEY_ID,
            "AKIA",
            AwsSecretKeyCredential.GRAVITINO_AWS_SECRET_ACCESS_KEY,
            "secret");
    CredentialProvider provider =
        CredentialProviderFactory.create(
            AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE, catalogProperties);
    Assertions.assertEquals(
        AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE, provider.credentialType());
    Optional<Credential> credential =
        provider.getCredentialOptional(new CatalogCredentialContext("u"));
    Assertions.assertTrue(credential.isPresent());
    AwsSecretKeyCredential aws = (AwsSecretKeyCredential) credential.get();
    Assertions.assertEquals("AKIA", aws.accessKeyId());
    Assertions.assertEquals("secret", aws.secretAccessKey());
    Assertions.assertEquals("AKIA", aws.credentialInfo().get("aws-access-key-id"));
    Assertions.assertEquals("secret", aws.credentialInfo().get("aws-secret-access-key"));
  }

  @Test
  void testMissingPropertiesReturnsNull() {
    CredentialProvider provider =
        CredentialProviderFactory.create(
            AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE, ImmutableMap.of());
    Assertions.assertTrue(
        provider.getCredentialOptional(new CatalogCredentialContext("u")).isEmpty());
  }

  @Test
  void testIncompletePairThrows() {
    RuntimeException e =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                CredentialProviderFactory.create(
                    AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE,
                    ImmutableMap.of(AwsSecretKeyCredential.GRAVITINO_AWS_ACCESS_KEY_ID, "AKIA")));
    Assertions.assertInstanceOf(IllegalArgumentException.class, e.getCause());
  }
}
