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

package org.apache.gravitino.s3.fs;

import static org.apache.gravitino.credential.S3TokenCredential.GRAVITINO_S3_SESSION_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.S3TokenCredential.GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY;
import static org.apache.gravitino.credential.S3TokenCredential.GRAVITINO_S3_TOKEN;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CredentialProvider implements AWSCredentialsProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3CredentialProvider.class);
  private final GravitinoClient client;
  private final String filesetIdentifier;
  private final Configuration configuration;

  private AWSCredentials basicSessionCredentials;
  private long expirationTime;

  public S3CredentialProvider(final URI uri, final Configuration conf) {
    this.filesetIdentifier =
        conf.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
    this.configuration = conf;
    GravitinoVirtualFileSystem gravitinoVirtualFileSystem = new GravitinoVirtualFileSystem();
    this.client = gravitinoVirtualFileSystem.initializeClient(conf);
  }

  @Override
  public AWSCredentials getCredentials() {
    // Refresh credentials if they are null or about to expire in 5 minutes
    if (basicSessionCredentials == null
        || System.currentTimeMillis() > expirationTime - 5 * 60 * 1000) {
      synchronized (this) {
        refresh();
      }
    }

    return basicSessionCredentials;
  }

  @Override
  public void refresh() {
    // The format of filesetIdentifier is "metalake.catalog.fileset.schema"
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    // Can't find any credential, use the default AKSK if possible.
    if (credentials.length == 0) {
      LOGGER.warn("No credential found for fileset: {}, try to use static AKSK", filesetIdentifier);
      expirationTime = Long.MAX_VALUE;
      this.basicSessionCredentials =
          new BasicAWSCredentials(
              configuration.get(Constants.ACCESS_KEY), configuration.get(Constants.SECRET_KEY));
      return;
    }

    Credential credential = getCredential(credentials);
    Map<String, String> credentialMap = credential.toProperties();

    String accessKeyId = credentialMap.get(GRAVITINO_S3_SESSION_ACCESS_KEY_ID);
    String secretAccessKey = credentialMap.get(GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY);

    if (S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE.equals(
        credentialMap.get(Credential.CREDENTIAL_TYPE))) {
      String sessionToken = credentialMap.get(GRAVITINO_S3_TOKEN);
      this.basicSessionCredentials =
          new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
    } else {
      this.basicSessionCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
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
   * @return The credential.
   */
  private Credential getCredential(Credential[] credentials) {
    for (Credential credential : credentials) {
      if (S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE.equals(credential.credentialType())) {
        return credential;
      }
    }

    // Not found, use the first one.
    return credentials[0];
  }
}
