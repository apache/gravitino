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
package org.apache.gravitino.encryption.kms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsApi {

  @Test
  void testParsesSupportedWireValues() {
    Assertions.assertEquals(KmsApi.AWS_KMS, KmsApi.fromWireValue("aws-kms"));
    Assertions.assertEquals(KmsApi.GOOGLE_CLOUD_KMS, KmsApi.fromWireValue(" GOOGLE-CLOUD-KMS "));
    Assertions.assertEquals("azure-key-vault", KmsApi.AZURE_KEY_VAULT.wireValue());
    Assertions.assertEquals(
        KmsApi.OPENBAO_TRANSIT, KmsApi.fromWireValue(KmsApi.OPENBAO_TRANSIT.wireValue()));
    Assertions.assertEquals(KmsApi.VAULT_TRANSIT, KmsApi.fromWireValue("vault-transit"));
  }

  @Test
  void testRejectsUnsupportedWireValues() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> KmsApi.fromWireValue(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> KmsApi.fromWireValue(" "));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> KmsApi.fromWireValue("custom-kms"));
    Assertions.assertTrue(exception.getMessage().contains("Unsupported KMS API 'custom-kms'"));
    Assertions.assertTrue(exception.getMessage().contains("aws-kms"));
    Assertions.assertTrue(exception.getMessage().contains("vault-transit"));
  }
}
