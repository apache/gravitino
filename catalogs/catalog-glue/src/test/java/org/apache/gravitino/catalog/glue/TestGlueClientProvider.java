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

import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

class TestGlueClientProvider {

  @Test
  void testBuildClientWithStaticCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "us-east-1");
    config.put(AWS_ACCESS_KEY_ID, "AKIAIOSFODNN7EXAMPLE");
    config.put(AWS_SECRET_ACCESS_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

    GlueClient client = GlueClientProvider.buildClient(config);
    assertNotNull(client);
    client.close();
  }

  @Test
  void testBuildClientWithDefaultCredentialChain() {
    // Without explicit credentials the default chain is used.
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "eu-west-1");

    GlueClient client = GlueClientProvider.buildClient(config);
    assertNotNull(client);
    client.close();
  }

  @Test
  void testBuildClientWithEndpointOverride() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "us-east-1");
    config.put(AWS_ACCESS_KEY_ID, "test");
    config.put(AWS_SECRET_ACCESS_KEY, "test");
    config.put(AWS_GLUE_ENDPOINT, "http://localhost:4566");

    GlueClient client = GlueClientProvider.buildClient(config);
    assertNotNull(client);
    client.close();
  }

  @Test
  void testBuildClientMissingRegionThrows() {
    Map<String, String> config = new HashMap<>();
    // No AWS_REGION set.

    assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
  }

  @Test
  void testBuildClientBlankRegionThrows() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "   ");

    assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
  }

  @Test
  void testBuildClientOnlyAccessKeyFallsBackToDefaultChain() {
    // Only one of the key pair provided → both must be present to use static creds.
    // Falls back to default chain, which just builds without error.
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "ap-southeast-1");
    config.put(AWS_ACCESS_KEY_ID, "AKIAIOSFODNN7EXAMPLE");
    // No AWS_SECRET_ACCESS_KEY → default chain is used instead.

    GlueClient client = GlueClientProvider.buildClient(config);
    assertNotNull(client);
    client.close();
  }
}
