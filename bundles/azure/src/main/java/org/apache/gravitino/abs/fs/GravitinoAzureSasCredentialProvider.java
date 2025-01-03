/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.abs.fs;

import static org.apache.gravitino.credential.ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE;
import static org.apache.gravitino.credential.ADLSTokenCredential.GRAVITINO_ADLS_SAS_TOKEN;
import static org.apache.gravitino.credential.AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY;
import static org.apache.gravitino.credential.AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoAzureSasCredentialProvider implements SASTokenProvider, Configurable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GravitinoAzureSasCredentialProvider.class);

  private Configuration configuration;

  private String filesetIdentifier;

  private GravitinoClient client;

  private String sasToken;

  private String azureStorageAccountName;
  private String azureStorageAccountKey;

  private long expirationTime;

  public String getAzureStorageAccountName() {
    return azureStorageAccountName;
  }

  public String getAzureStorageAccountKey() {
    return azureStorageAccountKey;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void initialize(Configuration conf, String accountName) throws IOException {
    this.filesetIdentifier =
        conf.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
    this.client = GravitinoVirtualFileSystemUtils.createClient(conf);
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    // Refresh credentials if they are null or about to expire in 5 minutes
    if (sasToken == null || System.currentTimeMillis() > expirationTime - 5 * 60 * 1000) {
      synchronized (this) {
        refresh();
      }
    }
    return sasToken;
  }

  private void refresh() {
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();
    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));

    Credential[] credentials = fileset.supportsCredentials().getCredentials();
    Optional<Credential> optionalCredential = getCredential(credentials);

    if (!optionalCredential.isPresent()) {
      LOGGER.warn("No credentials found for fileset {}", filesetIdentifier);
      return;
    }

    Credential credential = optionalCredential.get();
    Map<String, String> credentialMap = credential.toProperties();

    if (ADLS_TOKEN_CREDENTIAL_TYPE.equals(credentialMap.get(Credential.CREDENTIAL_TYPE))) {
      sasToken = credentialMap.get(GRAVITINO_ADLS_SAS_TOKEN);
    } else {
      azureStorageAccountName = credentialMap.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
      azureStorageAccountKey = credentialMap.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    }

    this.expirationTime = credential.expireTimeInMs();
    if (expirationTime <= 0) {
      expirationTime = Long.MAX_VALUE;
    }
  }

  /**
   * Get the credential from the credential array. Using dynamic credential first, if not found,
   * uses static credential.
   *
   * @param credentials The credential array.
   * @return An optional credential.
   */
  private Optional<Credential> getCredential(Credential[] credentials) {
    // Use dynamic credential if found.
    Optional<Credential> dynamicCredential =
        Arrays.stream(credentials)
            .filter(credential -> credential.credentialType().equals(ADLS_TOKEN_CREDENTIAL_TYPE))
            .findFirst();
    if (dynamicCredential.isPresent()) {
      return dynamicCredential;
    }

    // If dynamic credential not found, use the static one
    return Arrays.stream(credentials)
        .filter(
            credential ->
                credential
                    .credentialType()
                    .equals(AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE))
        .findFirst();
  }
}
