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

package org.apache.gravitino.cos.credential;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.storage.COSProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCOSCredentialProvider {

  @Test
  void testCredentialType() {
    COSSecretKeyProvider provider = new COSSecretKeyProvider();
    Assertions.assertEquals(
        COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE, provider.credentialType());
  }

  @Test
  void testSupportsScheme() {
    COSSecretKeyProvider provider = new COSSecretKeyProvider();
    Assertions.assertTrue(provider.supportsScheme("cosn"));
    Assertions.assertTrue(provider.supportsScheme("COSN"));
    Assertions.assertFalse(provider.supportsScheme("cos"));
    Assertions.assertFalse(provider.supportsScheme("oss"));
    Assertions.assertFalse(provider.supportsScheme("s3a"));
  }

  @Test
  void testInitializeAndGetCredential() {
    COSSecretKeyProvider provider = new COSSecretKeyProvider();
    provider.initialize(
        ImmutableMap.of(
            COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, "ak",
            COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, "sk"));

    Credential credential = provider.getCredential(null);

    Assertions.assertTrue(credential instanceof COSSecretKeyCredential);
    COSSecretKeyCredential typed = (COSSecretKeyCredential) credential;
    Assertions.assertEquals("ak", typed.accessKeyId());
    Assertions.assertEquals("sk", typed.secretAccessKey());
  }
}
