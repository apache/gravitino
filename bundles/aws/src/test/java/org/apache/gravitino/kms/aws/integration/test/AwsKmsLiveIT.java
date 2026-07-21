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
package org.apache.gravitino.kms.aws.integration.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.kms.aws.AwsKmsClientFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Optional read-only integration test against a live AWS KMS key. */
class AwsKmsLiveIT {

  private static final Logger LOG = LoggerFactory.getLogger(AwsKmsLiveIT.class);

  private static final String ENV_REGION = "GRAVITINO_AWS_KMS_LIVE_REGION";
  private static final String ENV_KEY_ID = "GRAVITINO_AWS_KMS_LIVE_KEY_ID";
  private static final String SOURCE = "live-aws-kms";

  private static String region;
  private static String keyId;

  @BeforeAll
  static void requireExplicitLiveFixture() {
    region = System.getenv(ENV_REGION);
    keyId = System.getenv(ENV_KEY_ID);

    List<String> missing = new ArrayList<>();
    if (StringUtils.isBlank(region)) {
      missing.add(ENV_REGION);
    }
    if (StringUtils.isBlank(keyId)) {
      missing.add(ENV_KEY_ID);
    }
    if (!missing.isEmpty()) {
      String message =
          "Skipping live AWS KMS IT; missing required environment variables: "
              + String.join(", ", missing);
      LOG.warn(message);
      Assumptions.assumeTrue(false, message);
    }
  }

  @Test
  void testDescribesConfiguredKeyWithoutCryptographicOperations() {
    AwsKmsClientFactory factory = new AwsKmsClientFactory();
    KmsReference reference = new KmsReference(KmsApi.AWS_KMS, SOURCE, keyId);

    try (KmsClient client =
        factory.create(
            SOURCE,
            Map.of(
                AwsKmsClientFactory.REGION,
                region,
                AwsKmsClientFactory.CREDENTIAL_METHOD,
                "default"))) {
      KmsKeyProperties properties = client.getKeyProperties(reference);

      Assertions.assertEquals(reference, properties.reference());
      Assertions.assertTrue(properties.present(), "The configured live AWS KMS key must exist");
    }
  }
}
