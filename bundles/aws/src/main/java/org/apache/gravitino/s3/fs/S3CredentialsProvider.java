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
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.hadoop.conf.Configuration;

public class S3CredentialsProvider implements AWSCredentialsProvider {
  private GravitinoFileSystemCredentialsProvider gravitinoFileSystemCredentialsProvider;

  private AWSCredentials basicSessionCredentials;
  private long expirationTime = Long.MAX_VALUE;
  private static final double EXPIRATION_TIME_FACTOR = 0.5D;

  public S3CredentialsProvider(final URI uri, final Configuration conf) {
    this.gravitinoFileSystemCredentialsProvider = FileSystemUtils.getGvfsCredentialProvider(conf);
  }

  @Override
  public AWSCredentials getCredentials() {
    // Refresh credentials if they are null or about to expire.
    if (basicSessionCredentials == null || System.currentTimeMillis() >= expirationTime) {
      synchronized (this) {
        refresh();
      }
    }

    return basicSessionCredentials;
  }

  @Override
  public void refresh() {
    Credential[] gravitinoCredentials = gravitinoFileSystemCredentialsProvider.getCredentials();
    Credential credential = S3Utils.getSuitableCredential(gravitinoCredentials);

    if (credential == null) {
      throw new RuntimeException("No suitable credential for S3 found...");
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
    } else if (credential instanceof AwsIrsaCredential) {
      AwsIrsaCredential awsIrsaCredential = (AwsIrsaCredential) credential;
      basicSessionCredentials =
          new BasicSessionCredentials(
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
}
