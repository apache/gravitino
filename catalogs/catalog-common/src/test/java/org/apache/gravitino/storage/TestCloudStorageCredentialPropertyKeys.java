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
package org.apache.gravitino.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestCloudStorageCredentialPropertyKeys {

  @Test
  void testOmitStaticCredentialProperties() {
    Map<String, String> input =
        Map.of(
            S3Properties.GRAVITINO_S3_ENDPOINT,
            "https://s3.amazonaws.com",
            S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
            CloudStorageCredentialPropertyKeys.MASKED_PROPERTY_VALUE,
            S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
            "secret",
            OSSProperties.GRAVITINO_OSS_REGION,
            "cn-hangzhou");

    Map<String, String> filtered =
        CloudStorageCredentialPropertyKeys.omitStaticCredentialProperties(input);

    assertEquals("https://s3.amazonaws.com", filtered.get(S3Properties.GRAVITINO_S3_ENDPOINT));
    assertEquals("cn-hangzhou", filtered.get(OSSProperties.GRAVITINO_OSS_REGION));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertFalse(filtered.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
  }

  @Test
  void testStaticCredentialKeyDetection() {
    assertTrue(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
    assertFalse(
        CloudStorageCredentialPropertyKeys.isStaticCredentialKey(
            COSProperties.GRAVITINO_COS_REGION));
  }
}
