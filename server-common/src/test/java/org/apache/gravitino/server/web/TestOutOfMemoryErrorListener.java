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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.Test;

/** Tests detection of errors that Jersey may wrap or map to ordinary responses. */
class TestOutOfMemoryErrorListener {
  @Test
  void recordsWrappedErrorsOnlyOnExceptionEvents() {
    ServerHealth health = new ServerHealth();
    OutOfMemoryErrorListener listener = new OutOfMemoryErrorListener(health);
    RequestEvent event = mock(RequestEvent.class);
    when(event.getException()).thenReturn(new RuntimeException(new OutOfMemoryError("Metaspace")));
    when(event.getType()).thenReturn(RequestEvent.Type.START);
    assertSame(listener, listener.onRequest(event));
    listener.onEvent(event);
    assertFalse(health.hasOutOfMemoryError());
    when(event.getType()).thenReturn(RequestEvent.Type.ON_EXCEPTION);
    listener.onEvent(event);
    assertTrue(health.hasOutOfMemoryError());
  }
}
