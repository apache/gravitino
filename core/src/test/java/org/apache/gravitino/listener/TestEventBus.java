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
package org.apache.gravitino.listener;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEventBus {

  @Test
  void testDispatchNullEventThrowsNullPointerException() {
    EventBus eventBus = new EventBus(Collections.emptyList());
    Assertions.assertThrows(NullPointerException.class, () -> eventBus.dispatchEvent(null));
  }

  // Test that dispatchEvent() automatically swallows exceptions for FailureEvents
  @Test
  void testDispatchEventSwallowsExceptionsForFailureEvents() {
    ThrowingEventListener listener =
        new ThrowingEventListener(new RuntimeException("Listener bug"));
    EventBus eventBus = new EventBus(Arrays.asList(listener));

    Event failureEvent =
        new FailureEvent("user", NameIdentifier.of("test"), new Exception("Original error")) {
          @Override
          public OperationStatus operationStatus() {
            return OperationStatus.SUCCESS;
          }
        };
    // RuntimeException should be swallowed and logged
    Assertions.assertDoesNotThrow(() -> eventBus.dispatchEvent(failureEvent));
  }

  // Test that dispatchEvent propagates ForbiddenException for PreEvents
  @Test
  void testDispatchEventPropagatesForbiddenExceptionForPreEvents() {
    ThrowingEventListener listener =
        new ThrowingEventListener(new ForbiddenException("Access denied"));
    EventBus eventBus = new EventBus(Arrays.asList(listener));

    PreEvent preEvent =
        new PreEvent("user", NameIdentifier.of("test")) {
          @Override
          public OperationStatus operationStatus() {
            return OperationStatus.SUCCESS;
          }
        };
    // ForbiddenException should be propagated
    Assertions.assertThrows(ForbiddenException.class, () -> eventBus.dispatchEvent(preEvent));
  }

  // Test that dispatchEvent propagates exceptions for regular (non-failure) events
  @Test
  void testDispatchEventPropagatesExceptionsForRegularEvents() {
    ThrowingEventListener listener =
        new ThrowingEventListener(new RuntimeException("Listener error"));
    EventBus eventBus = new EventBus(Arrays.asList(listener));

    Event postEvent =
        new Event("user", NameIdentifier.of("test")) {
          @Override
          public OperationStatus operationStatus() {
            return OperationStatus.SUCCESS;
          }
        };

    // RuntimeException should be propagated for regular events
    Assertions.assertThrows(RuntimeException.class, () -> eventBus.dispatchEvent(postEvent));
  }

  static class ThrowingEventListener implements EventListenerPlugin {
    private final RuntimeException exceptionToThrow;

    ThrowingEventListener(RuntimeException exceptionToThrow) {
      this.exceptionToThrow = exceptionToThrow;
    }

    @Override
    public void init(Map<String, String> properties) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void onPreEvent(PreEvent preEvent) {
      throw exceptionToThrow;
    }

    @Override
    public void onPostEvent(Event postEvent) {
      throw exceptionToThrow;
    }
  }
}
