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

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.hadoop.conf.Configuration;

/**
 * Fetches S3 credentials from Gravitino and refreshes them before they expire.
 *
 * <p>This class does not depend on any AWS SDK class, so that both {@link S3CredentialsProvider}
 * (AWS SDK v1, used by Hadoop 3.3) and {@link S3CredentialsProviderV2} (AWS SDK v2, used by Hadoop
 * 3.4 and later) can share it without requiring the other SDK on the classpath.
 */
class S3CredentialsRefresher {

  /** The point of the credential lifetime at which the credential is refreshed. */
  private static final double EXPIRATION_TIME_FACTOR = 0.5D;

  private final GravitinoFileSystemCredentialsProvider gravitinoFileSystemCredentialsProvider;
  private volatile S3Credentials credentials;
  private volatile long expirationTime = Long.MAX_VALUE;

  S3CredentialsRefresher(Configuration conf) {
    this.gravitinoFileSystemCredentialsProvider = FileSystemUtils.getGvfsCredentialProvider(conf);
  }

  /**
   * Returns the current credentials, refreshing them if they are missing or about to expire.
   *
   * @return the credentials
   */
  S3Credentials getCredentials() {
    if (credentials == null || System.currentTimeMillis() >= expirationTime) {
      synchronized (this) {
        refresh();
      }
    }
    return credentials;
  }

  @VisibleForTesting
  long expirationTime() {
    return expirationTime;
  }

  /** Fetches new credentials from Gravitino. */
  synchronized void refresh() {
    Credential[] gravitinoCredentials = gravitinoFileSystemCredentialsProvider.getCredentials();
    Credential credential = S3Utils.getSuitableCredential(gravitinoCredentials);
    if (credential == null) {
      throw new RuntimeException("No suitable credential for S3 found...");
    }

    if (credential instanceof S3SecretKeyCredential) {
      S3SecretKeyCredential s3SecretKeyCredential = (S3SecretKeyCredential) credential;
      credentials =
          new S3Credentials(
              s3SecretKeyCredential.accessKeyId(), s3SecretKeyCredential.secretAccessKey(), null);
    } else if (credential instanceof S3TokenCredential) {
      S3TokenCredential s3TokenCredential = (S3TokenCredential) credential;
      credentials =
          new S3Credentials(
              s3TokenCredential.accessKeyId(),
              s3TokenCredential.secretAccessKey(),
              s3TokenCredential.sessionToken());
    } else if (credential instanceof AwsIrsaCredential) {
      AwsIrsaCredential awsIrsaCredential = (AwsIrsaCredential) credential;
      credentials =
          new S3Credentials(
              awsIrsaCredential.accessKeyId(),
              awsIrsaCredential.secretAccessKey(),
              awsIrsaCredential.sessionToken());
    }

    if (credential.expireTimeInMs() > 0) {
      expirationTime =
          System.currentTimeMillis()
              + (long)
                  ((credential.expireTimeInMs() - System.currentTimeMillis())
                      * EXPIRATION_TIME_FACTOR);
    }
  }

  /** Immutable S3 credentials, with a session token for temporary credentials only. */
  static final class S3Credentials {
    private final String accessKeyId;
    private final String secretAccessKey;
    @Nullable private final String sessionToken;

    S3Credentials(String accessKeyId, String secretAccessKey, @Nullable String sessionToken) {
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
      this.sessionToken = sessionToken;
    }

    String accessKeyId() {
      return accessKeyId;
    }

    String secretAccessKey() {
      return secretAccessKey;
    }

    @Nullable
    String sessionToken() {
      return sessionToken;
    }
  }
}
