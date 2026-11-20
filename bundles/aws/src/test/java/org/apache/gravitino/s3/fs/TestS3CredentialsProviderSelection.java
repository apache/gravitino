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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests how {@link S3FileSystemProvider} picks and validates the S3A credentials provider. The
 * expected AWS SDK is passed by the build in the system property {@code s3a.expectedSdk}: "v1" when
 * the classpath carries Hadoop 3.3, "v2" when it carries Hadoop 3.4 or later.
 */
public class TestS3CredentialsProviderSelection {

  private static final String CREDENTIAL_KEY = "fs.s3a.aws.credentials.provider";
  private static final String SIMPLE = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  private static final String ANONYMOUS =
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
  private static final String V1_ENVIRONMENT =
      "com.amazonaws.auth.EnvironmentVariableCredentialsProvider";
  private static final String V2_ENVIRONMENT =
      "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider";

  private static final String V1_INTERFACE = "com.amazonaws.auth.AWSCredentialsProvider";
  private static final String V2_INTERFACE =
      "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider";

  private static final boolean EXPECT_V2 = "v2".equals(System.getProperty("s3a.expectedSdk"));

  private final S3FileSystemProvider provider = new S3FileSystemProvider();

  @Test
  void testExpectedSdkIsConfigured() {
    String expected = System.getProperty("s3a.expectedSdk");
    Assertions.assertTrue("v1".equals(expected) || "v2".equals(expected), "s3a.expectedSdk");
  }

  @Test
  void testHadoopSdkDetection() {
    Assertions.assertEquals(EXPECT_V2, S3Utils.isHadoopAwsSdkV2());
  }

  @Test
  void testProviderClassNamesMatchTheClasses() throws ClassNotFoundException {
    // Only the classes of the SDK in use can be loaded.
    if (EXPECT_V2) {
      Assertions.assertEquals(
          S3CredentialsProviderV2.class.getName(), S3FileSystemProvider.S3_CREDENTIALS_PROVIDER_V2);
      Assertions.assertTrue(S3Utils.isCredentialsProvider(S3CredentialsProviderV2.class));
    } else {
      Assertions.assertEquals(
          S3CredentialsProvider.class.getName(), S3FileSystemProvider.S3_CREDENTIALS_PROVIDER_V1);
      Assertions.assertTrue(S3Utils.isCredentialsProvider(S3CredentialsProvider.class));
    }
  }

  @Test
  void testVendedProviderMatchesHadoopSdk() {
    String expected =
        EXPECT_V2
            ? S3FileSystemProvider.S3_CREDENTIALS_PROVIDER_V2
            : S3FileSystemProvider.S3_CREDENTIALS_PROVIDER_V1;
    Credential[] secretKey = {new S3SecretKeyCredential("access-key", "secret-key")};
    Credential[] token = {
      new S3TokenCredential("access-key", "secret-key", "token", System.currentTimeMillis() + 1000)
    };
    Credential[] irsa = {
      new AwsIrsaCredential("access-key", "secret-key", "token", System.currentTimeMillis() + 1000)
    };

    for (Credential[] credentials : new Credential[][] {secretKey, token, irsa}) {
      Assertions.assertEquals(
          expected, provider.getFileSystemCredentialConf(credentials).get(CREDENTIAL_KEY));
    }
  }

  @Test
  void testNoVendedProviderWithoutS3Credential() {
    Assertions.assertTrue(provider.getFileSystemCredentialConf(new Credential[0]).isEmpty());
  }

  @Test
  void testValidProvidersAreKept() {
    Assertions.assertEquals(SIMPLE, check(SIMPLE));
    Assertions.assertEquals(ANONYMOUS, check(ANONYMOUS));
    Assertions.assertEquals(SIMPLE + "," + ANONYMOUS, check(SIMPLE + ", " + ANONYMOUS));
  }

  @Test
  void testAwsSdkV1ProviderNamesAreKept() {
    // Hadoop 3.4+ maps these names to AWS SDK v2 providers even without the AWS SDK v1, and Hadoop
    // 3.3 loads them from the AWS SDK v1. They must not be replaced in either case.
    Assertions.assertEquals(V1_ENVIRONMENT, check(V1_ENVIRONMENT));
    Assertions.assertEquals(SIMPLE + "," + V1_ENVIRONMENT, check(SIMPLE + "," + V1_ENVIRONMENT));
  }

  @Test
  void testAwsSdkV2ProviderName() {
    // AWS SDK v2 providers are only usable when the AWS SDK v2 is on the classpath.
    Assertions.assertEquals(
        isPresent(V2_INTERFACE) ? V2_ENVIRONMENT : SIMPLE, check(V2_ENVIRONMENT));
  }

  @Test
  void testProviderOfAnotherSdk() {
    // The class of an SDK that is not on the classpath cannot be loaded, so it is replaced.
    String other =
        EXPECT_V2
            ? S3FileSystemProvider.S3_CREDENTIALS_PROVIDER_V1
            : S3FileSystemProvider.S3_CREDENTIALS_PROVIDER_V2;
    String otherInterface = EXPECT_V2 ? V1_INTERFACE : V2_INTERFACE;
    Assertions.assertEquals(isPresent(otherInterface) ? other : SIMPLE, check(other));
  }

  @Test
  void testUnknownProviderFallsBackToDefault() {
    Assertions.assertEquals(SIMPLE, check("com.example.DoesNotExist"));
    Assertions.assertEquals(SIMPLE, check(SIMPLE + ",com.example.DoesNotExist"));
  }

  @Test
  void testClassThatIsNotAProviderFallsBackToDefault() {
    Assertions.assertEquals(SIMPLE, check("java.lang.String"));
  }

  @Test
  void testMissingKeyIsLeftUntouched() {
    Map<String, String> configs = new HashMap<>();
    provider.checkAndSetCredentialProvider(configs);
    Assertions.assertTrue(configs.isEmpty());
  }

  private static boolean isPresent(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private String check(String providers) {
    Map<String, String> configs = new HashMap<>();
    configs.put(CREDENTIAL_KEY, providers);
    provider.checkAndSetCredentialProvider(configs);
    return configs.get(CREDENTIAL_KEY);
  }
}
