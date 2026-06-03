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

package org.apache.gravitino.spark.connector.glue;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class TestGravitinoGlueCredentialsProvider {

  @Test
  void testCreateAndResolveValidCredentials() {
    AwsCredentialsProvider provider =
        GravitinoGlueCredentialsProvider.create(
            Map.of("access-key-id", "AKID", "secret-access-key", "SECRET"));
    AwsCredentials creds = provider.resolveCredentials();
    Assertions.assertEquals("AKID", creds.accessKeyId());
    Assertions.assertEquals("SECRET", creds.secretAccessKey());
  }

  @Test
  void testCreateWithMissingKeysThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> GravitinoGlueCredentialsProvider.create(Map.of()));
  }

  @Test
  void testCreateWithNullPropertiesThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> GravitinoGlueCredentialsProvider.create(null));
  }
}
