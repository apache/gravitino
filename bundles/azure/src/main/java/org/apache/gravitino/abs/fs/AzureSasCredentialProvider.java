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

import static org.apache.gravitino.credential.ADLSTokenCredential.GRAVITINO_ADLS_SAS_TOKEN;
import static org.apache.gravitino.credential.AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY;
import static org.apache.gravitino.credential.AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureSasCredentialProvider implements SASTokenProvider, Configurable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureSasCredentialProvider.class);

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
    GravitinoVirtualFileSystem gravitinoVirtualFileSystem = new GravitinoVirtualFileSystem();
    this.client = gravitinoVirtualFileSystem.initializeClient(conf);
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
    if (credentials.length == 0) {
      LOGGER.warn("No credentials found for fileset {}", filesetIdentifier);
      return;
    }

    // Use the first one.
    Credential credential = credentials[0];
    Map<String, String> credentialMap = credential.toProperties();

    if (ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE.equals(
        credentialMap.get(Credential.CREDENTIAL_TYPE))) {
      this.sasToken = credentialMap.get(GRAVITINO_ADLS_SAS_TOKEN);
    } else {
      this.azureStorageAccountName = credentialMap.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
      this.azureStorageAccountKey = credentialMap.get(GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    }

    this.expirationTime = credential.expireTimeInMs();
    if (expirationTime <= 0) {
      expirationTime = Long.MAX_VALUE;
    }
  }
}
