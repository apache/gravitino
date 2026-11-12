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

package org.apache.gravitino.listener.api.event;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link Event}'s automatic capture of the request's query parameters at construction
 * time. Uses {@link ListCatalogEvent} as a representative concrete subclass that does not override
 * {@code customInfo()} itself — any of the ~300 event classes without their own {@code customInfo}
 * go through the exact same {@link Event} constructor, so this one test stands in for all of them.
 */
public class TestEvent {

  @AfterEach
  void cleanup() {
    RequestContext.clear();
  }

  @Test
  void testCustomInfoCapturesQueryParamsAtConstructionTime() {
    RequestContext.setRequestQueryParams(ImmutableMap.of("details", "true"));
    Event event = new ListCatalogEvent("user", Namespace.of("metalake"), 3);
    Assertions.assertEquals("true", event.customInfo().get("details"));
  }

  @Test
  void testCustomInfoIsEmptyWhenNoQueryParamsCaptured() {
    Event event = new ListCatalogEvent("user", Namespace.of("metalake"), 3);
    Assertions.assertTrue(event.customInfo().isEmpty());
  }

  @Test
  void testCustomInfoSnapshotIsFixedAtConstructionNotAtReadTime() {
    RequestContext.setRequestQueryParams(ImmutableMap.of("details", "true"));
    Event event = new ListCatalogEvent("user", Namespace.of("metalake"), 3);
    RequestContext.setRequestQueryParams(ImmutableMap.of("details", "false"));
    Assertions.assertEquals(
        "true",
        event.customInfo().get("details"),
        "event must keep the snapshot taken at construction time, not a later thread-local value");
  }

  /**
   * customInfo() is final in Event precisely so this merge direction can't be gotten wrong by a
   * subclass: a subclass's own fact must win over an automatically captured query parameter of the
   * same name, otherwise a caller could overwrite an audit-critical field (e.g. a query parameter
   * literally named "http.status") just by naming a query parameter after it.
   */
  @Test
  void testOwnCustomInfoOverridesAutomaticQueryParamOnKeyCollision() {
    RequestContext.setRequestQueryParams(ImmutableMap.of("outcome", "attacker-supplied"));
    Event event =
        new Event("user", NameIdentifier.of("metalake")) {
          @Override
          protected Map<String, String> ownCustomInfo() {
            return ImmutableMap.of("outcome", "real-value");
          }
        };

    Assertions.assertEquals("real-value", event.customInfo().get("outcome"));
  }

  @Test
  void testOwnCustomInfoIsMergedAlongsideAutomaticQueryParams() {
    RequestContext.setRequestQueryParams(ImmutableMap.of("details", "true"));
    Event event =
        new Event("user", NameIdentifier.of("metalake")) {
          @Override
          protected Map<String, String> ownCustomInfo() {
            return ImmutableMap.of("http.status", "200");
          }
        };

    Assertions.assertEquals("true", event.customInfo().get("details"));
    Assertions.assertEquals("200", event.customInfo().get("http.status"));
  }
}
