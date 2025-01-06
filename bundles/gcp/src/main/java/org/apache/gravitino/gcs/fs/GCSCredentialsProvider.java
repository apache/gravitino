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
import java.util.Arrays;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSCredentialsProvider implements AccessTokenProvider {
  private static final Logger LOG = LoggerFactory.getLogger(GCSCredentialsProvider.class);
  private Configuration configuration;
  private GravitinoClient client;
  private String filesetIdentifier;

  private AccessToken accessToken;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.9D;

  @Override
  public AccessToken getAccessToken() {
    if (accessToken == null || System.currentTimeMillis() >= expirationTime) {
      try {
        refresh();
      } catch (IOException e) {
        LOG.error("Failed to refresh the access token", e);
      } finally {
        if (null != this.client) {
          this.client.close();
        }
      }
    }
    return accessToken;
  }

  @Override
  public void refresh() throws IOException {
    // The format of filesetIdentifier is "metalake.catalog.fileset.schema"
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    this.client = GravitinoVirtualFileSystemUtils.createClient(configuration);
    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    Credential credential = getCredential(credentials);
    // Can't find any credential, use the default one.
    if (null == credential) {
      LOG.warn(
          "No credential found for fileset: {}, try to use static JSON file", filesetIdentifier);
      return;
    }

    if (credential instanceof GCSTokenCredential) {
      GCSTokenCredential gcsTokenCredential = (GCSTokenCredential) credential;
      accessToken = new AccessToken(gcsTokenCredential.token(), credential.expireTimeInMs());

      if (credential.expireTimeInMs() > 0) {
        expirationTime =
            System.currentTimeMillis()
                + (long)
                    ((credential.expireTimeInMs() - System.currentTimeMillis())
                        * EXPIRATION_TIME_FACTOR);
      }
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    this.filesetIdentifier =
        configuration.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
  }

  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  /**
   * Get the credential from the credential array. Using dynamic credential first, if not found,
   * uses static credential.
   *
   * @param credentials The credential array.
   * @return An credential.
   */
  private Credential getCredential(Credential[] credentials) {
    // Use dynamic credential if found.
    Optional<Credential> optionalCredential =
        Arrays.stream(credentials)
            .filter(
                credential ->
                    credential
                        .credentialType()
                        .equals(GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE))
            .findFirst();

    return optionalCredential.orElse(null);
  }
}
