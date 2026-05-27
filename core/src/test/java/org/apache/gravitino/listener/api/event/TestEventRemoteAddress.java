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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEventRemoteAddress {

  @AfterEach
  public void cleanup() {
    RequestContext.clear();
  }

  @Test
  public void testRemoteAddressCapturedAtConstructionTime() {
    RequestContext.setRemoteAddress("10.0.0.1");
    Event event = new StubEvent();
    // Clear the ThreadLocal after construction to simulate what the servlet filter does.
    RequestContext.clear();

    Assertions.assertEquals(
        "10.0.0.1",
        event.remoteAddress(),
        "remoteAddress() must return the value captured at construction, not from ThreadLocal");
  }

  @Test
  public void testRemoteAddressDefaultsToUnknownWhenNotSet() {
    // No RequestContext set — must fall back to "unknown".
    Event event = new StubEvent();
    Assertions.assertEquals("unknown", event.remoteAddress());
  }

  @Test
  public void testRemoteAddressIsolatedAcrossInstances() {
    RequestContext.setRemoteAddress("1.2.3.4");
    Event first = new StubEvent();

    RequestContext.setRemoteAddress("5.6.7.8");
    Event second = new StubEvent();

    Assertions.assertEquals("1.2.3.4", first.remoteAddress());
    Assertions.assertEquals("5.6.7.8", second.remoteAddress());
  }

  // ---- stub ----

  static class StubEvent extends Event {
    StubEvent() {
      super("test-user", NameIdentifier.of("metalake", "catalog"));
    }

    @Override
    public OperationType operationType() {
      return OperationType.LIST_TABLE;
    }

    @Override
    public OperationStatus operationStatus() {
      return OperationStatus.SUCCESS;
    }

    @Override
    public EventSource eventSource() {
      return EventSource.GRAVITINO_SERVER;
    }
  }
}
