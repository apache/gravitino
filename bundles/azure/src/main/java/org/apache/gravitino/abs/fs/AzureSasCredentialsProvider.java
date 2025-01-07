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

import java.io.IOException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.ADLSTokenCredential;
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

public class AzureSasCredentialsProvider implements SASTokenProvider, Configurable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureSasCredentialsProvider.class);

  private Configuration configuration;

  private String filesetIdentifier;

  private GravitinoClient client;

  private String sasToken;

  private String azureStorageAccountName;
  private String azureStorageAccountKey;

  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.9D;

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
    this.configuration = conf;
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    // Refresh credentials if they are null or about to expire.
    if (sasToken == null || System.currentTimeMillis() >= expirationTime) {
      synchronized (this) {
        try {
          refresh();
        } finally {
          if (null != this.client) {
            this.client.close();
          }
        }
      }
    }
    return sasToken;
  }

  private void refresh() {
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    client = GravitinoVirtualFileSystemUtils.createClient(configuration);
    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();
    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));

    Credential[] credentials = fileset.supportsCredentials().getCredentials();
    Credential credential = getCredential(credentials);

    if (credential == null) {
      LOGGER.warn("No credentials found for fileset {}", filesetIdentifier);
      return;
    }

    if (credential instanceof ADLSTokenCredential) {
      ADLSTokenCredential adlsTokenCredential = (ADLSTokenCredential) credential;
      sasToken = adlsTokenCredential.sasToken();
    } else if (credential instanceof AzureAccountKeyCredential) {
      AzureAccountKeyCredential azureAccountKeyCredential = (AzureAccountKeyCredential) credential;
      azureStorageAccountName = azureAccountKeyCredential.accountName();
      azureStorageAccountKey = azureAccountKeyCredential.accountKey();
    }

    if (credential.expireTimeInMs() > 0) {
      expirationTime =
          System.currentTimeMillis()
              + (long)
                  ((credential.expireTimeInMs() - System.currentTimeMillis())
                      * EXPIRATION_TIME_FACTOR);
    }
  }

  /**
   * Get the credential from the credential array. Using dynamic credential first, if not found,
   * uses static credential.
   *
   * @param credentials The credential array.
   * @return A credential. Null if not found.
   */
  private Credential getCredential(Credential[] credentials) {
    // Use dynamic credential if found.
    for (Credential credential : credentials) {
      if (credential instanceof ADLSTokenCredential) {
        return credential;
      }
    }

    // If dynamic credential not found, use the static one
    for (Credential credential : credentials) {
      if (credential instanceof AzureAccountKeyCredential) {
        return credential;
      }
    }

    return null;
  }
}
