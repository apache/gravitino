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

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.credential.config.AzureCredentialConfig;
import org.apache.iceberg.azure.AdlsTokenCredentialProvider;

/** Supplies an Azure client-secret credential to Iceberg's ADLS FileIO. */
public class AzureClientSecretTokenCredentialProvider implements AdlsTokenCredentialProvider {

  @Nullable private TokenCredential credential;

  /** {@inheritDoc} */
  @Override
  public TokenCredential credential() {
    if (credential == null) {
      throw new IllegalStateException(
          "The Azure credential provider has not been initialized. Call initialize(properties) first.");
    }
    return credential;
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> properties) {
    AzureCredentialConfig config = new AzureCredentialConfig(properties);
    credential =
        new ClientSecretCredentialBuilder()
            .tenantId(config.tenantId())
            .clientId(config.clientId())
            .clientSecret(config.clientSecret())
            .build();
  }
}
