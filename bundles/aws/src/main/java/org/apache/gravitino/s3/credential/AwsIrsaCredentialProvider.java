/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.s3.credential;

import java.util.Map;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

public class AwsIrsaCredentialProvider implements CredentialProvider {

  private WebIdentityTokenFileCredentialsProvider credentialsProvider;

  @Override
  public void initialize(Map<String, String> properties) {
    // Use WebIdentityTokenFileCredentialsProvider for IRSA
    this.credentialsProvider = WebIdentityTokenFileCredentialsProvider.create();
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    AwsCredentials creds = credentialsProvider.resolveCredentials();
    if (creds instanceof AwsSessionCredentials) {
      AwsSessionCredentials sessionCreds = (AwsSessionCredentials) creds;
      long expiration =
          sessionCreds.expirationTime().isPresent()
              ? sessionCreds.expirationTime().get().toEpochMilli()
              : 0L;
      return new AwsIrsaCredential(
          sessionCreds.accessKeyId(),
          sessionCreds.secretAccessKey(),
          sessionCreds.sessionToken(),
          expiration);
    } else {
      throw new IllegalStateException(
          "AWS IRSA credentials must be of type AwsSessionCredentials. "
              + "Check your EKS/IRSA configuration. Got: "
              + creds.getClass().getName());
    }
  }
}
