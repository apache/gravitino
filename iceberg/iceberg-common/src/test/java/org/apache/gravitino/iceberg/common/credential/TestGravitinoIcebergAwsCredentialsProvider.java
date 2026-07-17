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
package org.apache.gravitino.iceberg.common.credential;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class TestGravitinoIcebergAwsCredentialsProvider {

  @Test
  void testApplyCredentialsUsesRefreshableProviderForExpiringS3Credentials() {
    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, TestRefreshingS3CredentialProvider.TYPE);
    sourceProperties.put("custom-hidden-property", "hidden-value");
    Map<String, String> targetProperties = new HashMap<>();

    IcebergServerCredentialUtils.applyCredentials(
        "catalog",
        new AwsIrsaCredential[] {
          new AwsIrsaCredential("access-key", "secret-key", "session-token", 123)
        },
        sourceProperties,
        targetProperties);

    Assertions.assertEquals(
        GravitinoIcebergAwsCredentialsProvider.class.getName(),
        targetProperties.get(IcebergServerCredentialUtils.CLIENT_CREDENTIALS_PROVIDER));
    Assertions.assertEquals(
        "catalog",
        targetProperties.get(
            IcebergServerCredentialUtils.CLIENT_CREDENTIALS_PROVIDER_PREFIX
                + GravitinoIcebergAwsCredentialsProvider.CATALOG_NAME));
    Assertions.assertEquals(
        TestRefreshingS3CredentialProvider.TYPE,
        targetProperties.get(
            IcebergServerCredentialUtils.CLIENT_CREDENTIALS_PROVIDER_PREFIX
                + CredentialConstants.CREDENTIAL_PROVIDERS));
    Assertions.assertEquals(
        "hidden-value",
        targetProperties.get(
            IcebergServerCredentialUtils.CLIENT_CREDENTIALS_PROVIDER_PREFIX
                + "custom-hidden-property"));
    Assertions.assertFalse(targetProperties.containsKey("s3.access-key-id"));
    Assertions.assertFalse(targetProperties.containsKey("s3.secret-access-key"));
    Assertions.assertFalse(targetProperties.containsKey("s3.session-token"));
  }

  @Test
  void testApplyCredentialsPrefersRefreshableProviderOverStaticS3Credentials() {
    Credential[] credentials =
        new Credential[] {
          new AwsIrsaCredential("access-key", "secret-key", "session-token", 123),
          new S3SecretKeyCredential("static-access-key", "static-secret-key")
        };
    Map<String, String> targetProperties = new HashMap<>();

    IcebergServerCredentialUtils.applyCredentials(
        "catalog", credentials, new HashMap<>(), targetProperties);

    Assertions.assertEquals(
        GravitinoIcebergAwsCredentialsProvider.class.getName(),
        targetProperties.get(IcebergServerCredentialUtils.CLIENT_CREDENTIALS_PROVIDER));
    Assertions.assertFalse(targetProperties.containsKey("s3.access-key-id"));
    Assertions.assertFalse(targetProperties.containsKey("s3.secret-access-key"));
    Assertions.assertFalse(targetProperties.containsKey("s3.session-token"));
  }

  @Test
  void testLocalProviderRefreshesExpiredCredential() {
    TestRefreshingS3CredentialProvider.reset();
    Map<String, String> properties = new HashMap<>();
    properties.put(
        GravitinoIcebergAwsCredentialsProvider.SOURCE,
        GravitinoIcebergAwsCredentialsProvider.SOURCE_LOCAL);
    properties.put(GravitinoIcebergAwsCredentialsProvider.CATALOG_NAME, "catalog");
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, TestRefreshingS3CredentialProvider.TYPE);
    properties.put(CredentialConstants.CREDENTIAL_CACHE_EXPIRE_RATIO, "0");

    TestRefreshingS3CredentialProvider.setExpireTimeInMs(System.currentTimeMillis() + 1);
    AwsCredentialsProvider provider = GravitinoIcebergAwsCredentialsProvider.create(properties);
    AwsSessionCredentials first = (AwsSessionCredentials) provider.resolveCredentials();

    TestRefreshingS3CredentialProvider.setExpireTimeInMs(System.currentTimeMillis() + 60_000);
    AwsSessionCredentials second = (AwsSessionCredentials) provider.resolveCredentials();

    Assertions.assertEquals("access-key-1", first.accessKeyId());
    Assertions.assertEquals("access-key-2", second.accessKeyId());
    Assertions.assertEquals(2, TestRefreshingS3CredentialProvider.generatedCount());
  }
}
