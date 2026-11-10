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
package org.apache.gravitino.catalog.glue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

class TestGravitinoGlueCredentialsProvider {

  @Test
  void testCreateAndResolveValidCredentials() {
    AwsCredentials credentials =
        GravitinoGlueCredentialsProvider.create(
                Map.of("access-key-id", "AKID", "secret-access-key", "SECRET"))
            .resolveCredentials();

    Assertions.assertInstanceOf(AwsBasicCredentials.class, credentials);
    Assertions.assertEquals("AKID", credentials.accessKeyId());
    Assertions.assertEquals("SECRET", credentials.secretAccessKey());
  }

  @Test
  void testExtraPropertiesAreIgnored() {
    AwsCredentials credentials =
        GravitinoGlueCredentialsProvider.create(
                Map.of("access-key-id", "AKID", "secret-access-key", "SECRET", "unused", "value"))
            .resolveCredentials();

    Assertions.assertEquals("AKID", credentials.accessKeyId());
    Assertions.assertEquals("SECRET", credentials.secretAccessKey());
  }

  @Test
  void testInvalidPropertiesAreRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> GravitinoGlueCredentialsProvider.create(null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoGlueCredentialsProvider.create(Map.of("secret-access-key", "SECRET")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoGlueCredentialsProvider.create(Map.of("access-key-id", "AKID")));

    Map<String, String> blankAccessKey = new HashMap<>();
    blankAccessKey.put("access-key-id", " ");
    blankAccessKey.put("secret-access-key", "SECRET");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoGlueCredentialsProvider.create(blankAccessKey));

    Map<String, String> blankSecretKey = new HashMap<>();
    blankSecretKey.put("access-key-id", "AKID");
    blankSecretKey.put("secret-access-key", " ");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoGlueCredentialsProvider.create(blankSecretKey));
  }
}
