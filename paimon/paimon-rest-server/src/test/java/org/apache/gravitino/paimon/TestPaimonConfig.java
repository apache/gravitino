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
package org.apache.gravitino.paimon;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonConfig {

  @Test
  public void testDefaultPortsAreOverwritten() {
    PaimonConfig config = new PaimonConfig(ImmutableMap.of());
    Map<String, String> overwrite = config.getOverwriteDefaultConfig();
    Assertions.assertEquals(
        String.valueOf(PaimonConfig.DEFAULT_PAIMON_REST_SERVICE_HTTP_PORT),
        overwrite.get("httpPort"));
    Assertions.assertEquals(
        String.valueOf(PaimonConfig.DEFAULT_PAIMON_REST_SERVICE_HTTPS_PORT),
        overwrite.get("httpsPort"));
  }

  @Test
  public void testExtensionPackagesAreParsedAsSequence() {
    PaimonConfig config =
        new PaimonConfig(
            ImmutableMap.of(PaimonConfig.PAIMON_REST_EXTENSION_PACKAGES, "a.b.c, d.e.f"));
    Assertions.assertEquals(2, config.get(PaimonConfig.REST_API_EXTENSION_PACKAGES).size());
  }

  @Test
  public void testUnknownKeysAreKeptVerbatim() {
    PaimonConfig config =
        new PaimonConfig(ImmutableMap.of("catalog-backend", "jdbc", "warehouse", "/tmp/warehouse"));
    Assertions.assertEquals("jdbc", config.getAllConfig().get("catalog-backend"));
    Assertions.assertEquals("/tmp/warehouse", config.getAllConfig().get("warehouse"));
  }
}
