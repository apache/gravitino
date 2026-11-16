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
package org.apache.gravitino.iceberg.service.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** HTTP coverage of a custom idempotency key lifetime. */
public class TestIcebergIdempotencyConfig extends IcebergTestBase {
  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(
        IcebergConfigOperations.class,
        true,
        Collections.emptyList(),
        ImmutableMap.of(
            IcebergConstants.ICEBERG_IDEMPOTENCY_ENABLED,
            "true",
            IcebergConstants.ICEBERG_IDEMPOTENCY_KEY_LIFETIME,
            "PT2H"));
  }

  @Test
  void testCustomLifetimeIsAdvertisedAtTopLevel() {
    try (Response response = getConfigClientBuilder().get()) {
      Assertions.assertEquals(200, response.getStatus());
      JsonNode config = response.readEntity(JsonNode.class);
      Assertions.assertEquals("PT2H", config.get("idempotency-key-lifetime").asText());
      Assertions.assertFalse(config.get("defaults").has("idempotency-key-lifetime"));
      Assertions.assertFalse(config.get("overrides").has("idempotency-key-lifetime"));
    }
  }
}
