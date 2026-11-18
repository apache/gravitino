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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests the sticky out-of-memory state and cause-chain handling. */
class TestServerHealth {
  @Test
  void ordinaryFailuresDoNotPoisonHealth() {
    ServerHealth health = new ServerHealth();
    health.recordFailure(null);
    health.recordFailure(new IllegalStateException("database unavailable"));
    health.recordFailure(new StackOverflowError());
    health.recordFailure(new NoClassDefFoundError("missing connector"));
    assertFalse(health.hasOutOfMemoryError());
  }

  @Test
  void heapAndMetaspaceFailuresRemainRecorded() {
    for (String message :
        new String[] {"Java heap space", "Metaspace", "unable to create native thread"}) {
      ServerHealth health = new ServerHealth();
      health.recordFailure(new RuntimeException(new OutOfMemoryError(message)));
      health.recordFailure(null);
      health.recordFailure(new IllegalArgumentException());
      assertTrue(health.hasOutOfMemoryError());
    }
  }

  @Test
  void cyclicCausesTerminateAndStillFindOutOfMemory() {
    Throwable first = new RuntimeException();
    Throwable second = new RuntimeException(first);
    first.initCause(second);
    ServerHealth health = new ServerHealth();
    health.recordFailure(first);
    assertFalse(health.hasOutOfMemoryError());

    Throwable root = new RuntimeException();
    Throwable oom = new OutOfMemoryError("Metaspace");
    Throwable middle = new RuntimeException(oom);
    root.initCause(middle);
    oom.initCause(root);
    health.recordFailure(root);
    assertTrue(health.hasOutOfMemoryError());
  }
}
