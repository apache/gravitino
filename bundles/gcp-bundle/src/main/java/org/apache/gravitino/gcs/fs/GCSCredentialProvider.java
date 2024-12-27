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

package org.apache.gravitino.gcs.fs;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSCredentialProvider implements AccessTokenProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCSCredentialProvider.class);
  private Configuration configuration;
  private GravitinoClient client;
  private String filesetIdentifier;

  private AccessToken accessToken;
  private long expirationTime;

  @Override
  public AccessToken getAccessToken() {
    if (accessToken == null || expirationTime < System.currentTimeMillis() + 5 * 60 * 1000) {
      try {
        refresh();
      } catch (IOException e) {
        LOGGER.error("Failed to refresh the access token", e);
      }
    }
    return accessToken;
  }

  @Override
  public void refresh() throws IOException {
    // Refresh credentials if they are null or about to expire in 5 minutes
    // The format of filesetIdentifier is "metalake.catalog.fileset.schema"
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    // Can't find any credential, use the default one.
    if (credentials.length == 0) {
      LOGGER.warn(
          "No credential found for fileset: {}, try to use static JSON file", filesetIdentifier);
      return;
    }

    Credential credential = credentials[0];
    Map<String, String> credentialMap = credential.toProperties();

    if (GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE.equals(
        credentialMap.get(Credential.CREDENTIAL_TYPE))) {
      String sessionToken = credentialMap.get(GCSTokenCredential.GCS_TOKEN_NAME);
      accessToken = new AccessToken(sessionToken, expirationTime);
    }

    this.expirationTime = credential.expireTimeInMs();
    if (expirationTime <= 0) {
      expirationTime = Long.MAX_VALUE;
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;

    this.filesetIdentifier =
        configuration.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
    // extra value and init Gravitino client here
    GravitinoVirtualFileSystem gravitinoVirtualFileSystem = new GravitinoVirtualFileSystem();
    this.client = gravitinoVirtualFileSystem.initializeClient(configuration);
  }

  @Override
  public Configuration getConf() {
    return this.configuration;
  }
}
