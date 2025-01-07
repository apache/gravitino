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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import java.net.URI;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CredentialsProvider implements AWSCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(S3CredentialsProvider.class);
  private GravitinoClient client;
  private final String filesetIdentifier;
  private final Configuration configuration;

  private AWSCredentials basicSessionCredentials;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.9D;

  public S3CredentialsProvider(final URI uri, final Configuration conf) {
    this.filesetIdentifier =
        conf.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
    this.configuration = conf;
  }

  @Override
  public AWSCredentials getCredentials() {
    // Refresh credentials if they are null or about to expire.
    if (basicSessionCredentials == null || System.currentTimeMillis() >= expirationTime) {
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

    return basicSessionCredentials;
  }

  @Override
  public void refresh() {
    // The format of filesetIdentifier is "metalake.catalog.fileset.schema"
    String[] idents = filesetIdentifier.split("\\.");
    String catalog = idents[1];

    client = GravitinoVirtualFileSystemUtils.createClient(configuration);
    FilesetCatalog filesetCatalog = client.loadCatalog(catalog).asFilesetCatalog();

    Fileset fileset = filesetCatalog.loadFileset(NameIdentifier.of(idents[2], idents[3]));
    Credential[] credentials = fileset.supportsCredentials().getCredentials();
    Credential credential = getCredential(credentials);

    // Can't find any credential, use the default AKSK if possible.
    if (credential == null) {
      LOG.warn("No credential found for fileset: {}, try to use static AKSK", filesetIdentifier);
      expirationTime = Long.MAX_VALUE;
      this.basicSessionCredentials =
          new BasicAWSCredentials(
              configuration.get(Constants.ACCESS_KEY), configuration.get(Constants.SECRET_KEY));
      return;
    }

    if (credential instanceof S3SecretKeyCredential) {
      S3SecretKeyCredential s3SecretKeyCredential = (S3SecretKeyCredential) credential;
      basicSessionCredentials =
          new BasicAWSCredentials(
              s3SecretKeyCredential.accessKeyId(), s3SecretKeyCredential.secretAccessKey());
    } else if (credential instanceof S3TokenCredential) {
      S3TokenCredential s3TokenCredential = (S3TokenCredential) credential;
      basicSessionCredentials =
          new BasicSessionCredentials(
              s3TokenCredential.accessKeyId(),
              s3TokenCredential.secretAccessKey(),
              s3TokenCredential.sessionToken());
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
      if (credential instanceof S3TokenCredential) {
        return credential;
      }
    }

    // If dynamic credential not found, use the static one
    for (Credential credential : credentials) {
      if (credential instanceof S3SecretKeyCredential) {
        return credential;
      }
    }
    return null;
  }
}
