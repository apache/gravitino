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
package org.apache.gravitino.abs.credential;

import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.config.AzureCredentialConfig;

/** Generates ADLS token to access ADLS data. */
public class ADLSTokenGenerator implements CredentialGenerator<ADLSTokenCredential> {

  @Override
  public ADLSTokenCredential generate(Map<String, String> properties, CredentialContext context) {
    PathBasedCredentialContext pathContext = (PathBasedCredentialContext) context;
    AzureCredentialConfig config = new AzureCredentialConfig(properties);

    Set<String> writePaths = pathContext.getWritePaths();
    Set<String> readPaths = pathContext.getReadPaths();
    Set<String> combinedPaths = new HashSet<>(writePaths);
    combinedPaths.addAll(readPaths);

    if (combinedPaths.size() != 1) {
      throw new IllegalArgumentException(
          "ADLS should contain exactly one unique path, but found: "
              + combinedPaths.size()
              + " paths: "
              + combinedPaths);
    }
    String uniquePath = combinedPaths.iterator().next();

    String endpoint =
        String.format(
            "https://%s.%s", config.storageAccountName(), ADLSTokenCredential.ADLS_DOMAIN);

    ClientSecretCredential clientSecretCredential =
        new ClientSecretCredentialBuilder()
            .tenantId(config.tenantId())
            .clientId(config.clientId())
            .clientSecret(config.clientSecret())
            .build();

    DataLakeServiceClient dataLakeServiceClient =
        new DataLakeServiceClientBuilder()
            .endpoint(endpoint)
            .credential(clientSecretCredential)
            .buildClient();

    OffsetDateTime start = OffsetDateTime.now();
    OffsetDateTime expiry = OffsetDateTime.now().plusSeconds(config.adlsTokenExpireInSecs());
    UserDelegationKey userDelegationKey = dataLakeServiceClient.getUserDelegationKey(start, expiry);

    PathSasPermission pathSasPermission =
        new PathSasPermission().setReadPermission(true).setListPermission(true);
    if (!writePaths.isEmpty()) {
      pathSasPermission
          .setWritePermission(true)
          .setDeletePermission(true)
          .setCreatePermission(true)
          .setAddPermission(true);
    }

    DataLakeServiceSasSignatureValues signatureValues =
        new DataLakeServiceSasSignatureValues(expiry, pathSasPermission);
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(uniquePath);
    String sasToken =
        new DataLakeSasImplUtil(
                signatureValues,
                locationParts.getContainer(),
                ADLSLocationUtils.trimSlashes(locationParts.getPath()),
                true)
            .generateUserDelegationSas(
                userDelegationKey, locationParts.getAccountName(), Context.NONE);

    return new ADLSTokenCredential(
        locationParts.getAccountName(), sasToken, expiry.toInstant().toEpochMilli());
  }
}
