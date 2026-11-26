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

import java.net.URI;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.InMemoryFileSystemCredentialsProvider;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Tests the complete in-memory credential vending path for S3 credentials with AWS SDK v2. */
public class TestS3CredentialVendingV2 {

  private static final URI S3_URI = URI.create("s3a://test-bucket/");
  private static final long EXPIRATION_TIME = System.currentTimeMillis() + 60_000L;

  @Test
  void testSecretKeyCredential() {
    S3SecretKeyCredential credential = new S3SecretKeyCredential("access-key", "secret-key");
    AwsCredentials actual = readCredential(credential);

    Assertions.assertEquals("access-key", actual.accessKeyId());
    Assertions.assertEquals("secret-key", actual.secretAccessKey());
    Assertions.assertFalse(actual instanceof AwsSessionCredentials);
  }

  @Test
  void testTokenCredential() {
    S3TokenCredential credential =
        new S3TokenCredential("access-key", "secret-key", "session-token", EXPIRATION_TIME);
    AwsCredentials actual = readCredential(credential);

    Assertions.assertTrue(actual instanceof AwsSessionCredentials);
    Assertions.assertEquals("access-key", actual.accessKeyId());
    Assertions.assertEquals("secret-key", actual.secretAccessKey());
    Assertions.assertEquals("session-token", ((AwsSessionCredentials) actual).sessionToken());
  }

  @Test
  void testIrsaCredential() {
    AwsIrsaCredential credential =
        new AwsIrsaCredential("access-key", "secret-key", "session-token", EXPIRATION_TIME);
    AwsCredentials actual = readCredential(credential);

    Assertions.assertTrue(actual instanceof AwsSessionCredentials);
    Assertions.assertEquals("access-key", actual.accessKeyId());
    Assertions.assertEquals("secret-key", actual.secretAccessKey());
    Assertions.assertEquals("session-token", ((AwsSessionCredentials) actual).sessionToken());
  }

  private AwsCredentials readCredential(Credential credential) {
    Credential[] credentials = new Credential[] {credential};
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    try {
      Configuration conf = new Configuration(false);
      conf.set(
          GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
          InMemoryFileSystemCredentialsProvider.class.getCanonicalName());
      conf.set(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);
      return new S3CredentialsProviderV2(S3_URI, conf).resolveCredentials();
    } finally {
      InMemoryFileSystemCredentialsProvider.unregister(handle);
    }
  }
}
