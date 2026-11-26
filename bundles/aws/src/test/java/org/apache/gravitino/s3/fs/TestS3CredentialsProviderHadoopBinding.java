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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.InMemoryFileSystemCredentialsProvider;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Checks that the S3A connector of Hadoop 3.4 creates the provider that {@link
 * S3FileSystemProvider} vends and reads the Gravitino credentials from it.
 */
public class TestS3CredentialsProviderHadoopBinding {

  private static final URI S3_URI = URI.create("s3a://test-bucket/");

  @Test
  void testSecretKeyCredential() throws IOException {
    AwsCredentials actual =
        resolveWithHadoop(new S3SecretKeyCredential("access-key", "secret-key"));

    Assertions.assertEquals("access-key", actual.accessKeyId());
    Assertions.assertEquals("secret-key", actual.secretAccessKey());
    Assertions.assertFalse(actual instanceof AwsSessionCredentials);
  }

  @Test
  void testTokenCredential() throws IOException {
    AwsCredentials actual =
        resolveWithHadoop(
            new S3TokenCredential(
                "access-key", "secret-key", "session-token", System.currentTimeMillis() + 60_000L));

    Assertions.assertTrue(actual instanceof AwsSessionCredentials);
    Assertions.assertEquals("access-key", actual.accessKeyId());
    Assertions.assertEquals("secret-key", actual.secretAccessKey());
    Assertions.assertEquals("session-token", ((AwsSessionCredentials) actual).sessionToken());
  }

  private AwsCredentials resolveWithHadoop(Credential credential) throws IOException {
    Credential[] credentials = new Credential[] {credential};
    String handle = InMemoryFileSystemCredentialsProvider.register(credentials);
    try {
      Configuration conf = new Configuration(false);
      Map<String, String> credentialConf =
          new S3FileSystemProvider().getFileSystemCredentialConf(credentials);
      credentialConf.forEach(conf::set);
      conf.set(
          GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
          InMemoryFileSystemCredentialsProvider.class.getCanonicalName());
      conf.set(InMemoryFileSystemCredentialsProvider.CREDENTIAL_HANDLE, handle);
      Assertions.assertEquals(
          S3CredentialsProviderV2.class.getCanonicalName(),
          conf.get("fs.s3a.aws.credentials.provider"));

      try (AWSCredentialProviderList providers =
          CredentialProviderListFactory.createAWSCredentialProviderList(S3_URI, conf)) {
        return providers.resolveCredentials();
      }
    } finally {
      InMemoryFileSystemCredentialsProvider.unregister(handle);
    }
  }
}
