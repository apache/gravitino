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
package org.apache.gravitino.iceberg.service.sign;

import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Converts Gravitino credentials to AWS SDK credentials for S3 request signing. */
public final class AwsSigningCredentials {

  private AwsSigningCredentials() {}

  /**
   * Converts a Gravitino credential to AWS SDK credentials.
   *
   * @param credential Gravitino credential used to sign S3 requests
   * @return AWS SDK credentials
   * @throws UnsupportedOperationException if the credential type cannot sign S3 requests
   */
  public static AwsCredentials toAwsCredentials(Credential credential) {
    if (credential instanceof S3SecretKeyCredential) {
      S3SecretKeyCredential secretKeyCredential = (S3SecretKeyCredential) credential;
      return AwsBasicCredentials.create(
          secretKeyCredential.accessKeyId(), secretKeyCredential.secretAccessKey());
    }
    if (credential instanceof S3TokenCredential) {
      S3TokenCredential tokenCredential = (S3TokenCredential) credential;
      return AwsSessionCredentials.create(
          tokenCredential.accessKeyId(),
          tokenCredential.secretAccessKey(),
          tokenCredential.sessionToken());
    }
    if (credential instanceof AwsIrsaCredential) {
      AwsIrsaCredential irsaCredential = (AwsIrsaCredential) credential;
      return AwsSessionCredentials.create(
          irsaCredential.accessKeyId(),
          irsaCredential.secretAccessKey(),
          irsaCredential.sessionToken());
    }
    throw new UnsupportedOperationException(
        "Credential type does not support S3 remote signing: " + credential.credentialType());
  }
}
