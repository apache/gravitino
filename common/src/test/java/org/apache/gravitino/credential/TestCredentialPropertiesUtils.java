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

package org.apache.gravitino.credential;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialPropertiesUtils {

  @Test
  void testToIcebergProperties() {
    S3TokenCredential s3TokenCredential = new S3TokenCredential("key", "secret", "token", 0);
    Map<String, String> icebergProperties =
        CredentialPropertyUtils.toIcebergProperties(s3TokenCredential);
    Map<String, String> expectedProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_S3_ACCESS_KEY_ID,
            "key",
            CredentialPropertyUtils.ICEBERG_S3_SECRET_ACCESS_KEY,
            "secret",
            CredentialPropertyUtils.ICEBERG_S3_TOKEN,
            "token");
    Assertions.assertEquals(expectedProperties, icebergProperties);

    S3SecretKeyCredential secretKeyCredential = new S3SecretKeyCredential("key", "secret");
    icebergProperties = CredentialPropertyUtils.toIcebergProperties(secretKeyCredential);
    expectedProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_S3_ACCESS_KEY_ID,
            "key",
            CredentialPropertyUtils.ICEBERG_S3_SECRET_ACCESS_KEY,
            "secret");
    Assertions.assertEquals(expectedProperties, icebergProperties);
  }
}
