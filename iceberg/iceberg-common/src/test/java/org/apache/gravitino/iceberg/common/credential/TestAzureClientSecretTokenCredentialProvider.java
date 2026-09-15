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

package org.apache.gravitino.iceberg.common.credential;

import com.azure.identity.ClientSecretCredential;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.iceberg.azure.AdlsTokenCredentialProvider;
import org.apache.iceberg.azure.AdlsTokenCredentialProviders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAzureClientSecretTokenCredentialProvider {

  @Test
  void testCredentialBeforeInitialize() {
    AzureClientSecretTokenCredentialProvider provider =
        new AzureClientSecretTokenCredentialProvider();

    IllegalStateException exception =
        Assertions.assertThrows(IllegalStateException.class, provider::credential);
    Assertions.assertEquals(
        "The Azure credential provider has not been initialized. Call initialize(properties) first.",
        exception.getMessage());
  }

  @Test
  void testLoadAndInitializeProviderThroughIceberg() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
            "account",
            AzureProperties.GRAVITINO_AZURE_TENANT_ID,
            "tenant",
            AzureProperties.GRAVITINO_AZURE_CLIENT_ID,
            "client",
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET,
            "secret");
    Map<String, String> icebergProperties =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProperties);

    AdlsTokenCredentialProvider provider = AdlsTokenCredentialProviders.from(icebergProperties);

    Assertions.assertInstanceOf(AzureClientSecretTokenCredentialProvider.class, provider);
    Assertions.assertInstanceOf(ClientSecretCredential.class, provider.credential());
    Assertions.assertSame(provider.credential(), provider.credential());
  }
}
