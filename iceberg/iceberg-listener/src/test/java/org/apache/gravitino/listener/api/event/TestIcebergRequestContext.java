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

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergRequestContext {

  @Test
  void testNoHeaderIsSync() {
    Assertions.assertFalse(context(Collections.emptyMap()).asyncPurge());
  }

  @Test
  void testTrueHeaderIsAsync() {
    Assertions.assertTrue(
        context(Map.of(IcebergRequestContext.ASYNC_PURGE_HEADER, " true ")).asyncPurge());
  }

  @Test
  void testFalseHeaderIsSync() {
    Assertions.assertFalse(
        context(Map.of(IcebergRequestContext.ASYNC_PURGE_HEADER, "false")).asyncPurge());
  }

  @Test
  void testGarbageHeaderIsSync() {
    Assertions.assertFalse(
        context(Map.of(IcebergRequestContext.ASYNC_PURGE_HEADER, "yes")).asyncPurge());
  }

  @Test
  void testUppercaseValueIsSync() {
    // HTTP header values are case-sensitive; only the exact value "true" opts in.
    Assertions.assertFalse(
        context(Map.of(IcebergRequestContext.ASYNC_PURGE_HEADER, "True")).asyncPurge());
  }

  @Test
  void testHeaderNameIsCaseInsensitive() {
    Assertions.assertTrue(
        context(Map.of("x-gravitino-async-purge", "true")).asyncPurge());
  }

  private static IcebergRequestContext context(Map<String, String> headers) {
    return new IcebergRequestContext("cat", "user", "localhost", headers, false);
  }
}
