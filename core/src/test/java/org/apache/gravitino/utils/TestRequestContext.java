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

package org.apache.gravitino.utils;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRequestContext {

  @AfterEach
  public void cleanup() {
    RequestContext.clear();
  }

  @Test
  public void testSetAndGet() {
    RequestContext.setRemoteAddress("192.168.1.1");
    Assertions.assertEquals("192.168.1.1", RequestContext.getRemoteAddress());
  }

  @Test
  public void testClearRemovesValue() {
    RequestContext.setRemoteAddress("10.0.0.1");
    RequestContext.clear();
    Assertions.assertNull(RequestContext.getRemoteAddress());
  }

  @Test
  public void testGetReturnsNullWhenNotSet() {
    Assertions.assertNull(RequestContext.getRemoteAddress());
  }

  @Test
  public void testThreadIsolation() throws InterruptedException {
    RequestContext.setRemoteAddress("main-thread-ip");
    AtomicReference<String> childValue = new AtomicReference<>();

    Thread child =
        new Thread(
            () -> {
              // Child thread has its own ThreadLocal slot — must not see the main-thread value.
              childValue.set(RequestContext.getRemoteAddress());
            });
    child.start();
    child.join();

    Assertions.assertNull(childValue.get(), "Child thread should not inherit parent ThreadLocal");
    Assertions.assertEquals(
        "main-thread-ip", RequestContext.getRemoteAddress(), "Main thread value unchanged");
  }
}
