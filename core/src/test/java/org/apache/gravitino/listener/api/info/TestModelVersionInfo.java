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
package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link ModelVersionInfo}. */
public class TestModelVersionInfo {

  @Test
  public void testNullUrisYieldEmptyMap() {
    ModelVersionInfo info =
        new ModelVersionInfo((Map<String, String>) null, null, null, null, null);

    // Mirrors the uri-based constructor and the properties handling: absent uris
    // surface as an empty map, not null, and uri() must not throw.
    Assertions.assertNotNull(info.uris());
    Assertions.assertTrue(info.uris().isEmpty());
    Assertions.assertNull(info.uri());
  }

  @Test
  public void testUrisAreDefensivelyCopied() {
    Map<String, String> uris = new HashMap<>();
    uris.put("unknown", "gs://bucket/model/v1");
    ModelVersionInfo info = new ModelVersionInfo(uris, null, null, null, null);

    uris.put("unknown", "gs://bucket/model/MUTATED");
    uris.put("extra", "gs://bucket/model/EXTRA");

    // Event payloads are read-only; mutating the caller's map must not change them.
    Assertions.assertEquals(
        ImmutableMap.of("unknown", "gs://bucket/model/v1"), ImmutableMap.copyOf(info.uris()));
    Assertions.assertEquals("gs://bucket/model/v1", info.uri());
  }
}
