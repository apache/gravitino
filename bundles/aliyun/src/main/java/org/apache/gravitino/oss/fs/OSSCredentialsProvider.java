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

package org.apache.gravitino.oss.fs;

import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import java.net.URI;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSSCredentialsProvider implements CredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(OSSCredentialsProvider.class);
  private Credentials basicCredentials;
  private final String filesetIdentifier;
  private GravitinoClient client;
  private final Configuration configuration;

  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.9D;

  public OSSCredentialsProvider(URI uri, Configuration conf) {
    this.filesetIdentifier =
        conf.get(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_IDENTIFIER);
    this.configuration = conf;
  }

  @Override
  public void setCredentials(Credentials credentials) {}

  @Override
  public Credentials getCredentials() {
    // If the credentials are null or about to expire, refresh the credentials.
    if (basicCredentials == null || System.currentTimeMillis() >= expirationTime) {
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

    return basicCredentials;
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
      LOG.warn("No credential found for fileset: {}, try to use static AKSK", filesetIdentifier);
      expirationTime = Long.MAX_VALUE;
      this.basicCredentials =
          new DefaultCredentials(
              configuration.get(Constants.ACCESS_KEY_ID),
              configuration.get(Constants.ACCESS_KEY_SECRET));
      return;
    }

    if (credential instanceof OSSSecretKeyCredential) {
      OSSSecretKeyCredential ossSecretKeyCredential = (OSSSecretKeyCredential) credential;
      basicCredentials =
          new DefaultCredentials(
              ossSecretKeyCredential.accessKeyId(), ossSecretKeyCredential.secretAccessKey());
    } else if (credential instanceof OSSTokenCredential) {
      OSSTokenCredential ossTokenCredential = (OSSTokenCredential) credential;
      basicCredentials =
          new BasicCredentials(
              ossTokenCredential.accessKeyId(),
              ossTokenCredential.secretAccessKey(),
              ossTokenCredential.securityToken());
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
      if (credential.credentialType().equals(OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE)) {
        return credential;
      }
    }

    // If dynamic credential not found, use the static one
    for (Credential credential : credentials) {
      if (credential
          .credentialType()
          .equals(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE)) {
        return credential;
      }
    }

    return null;
  }
}
