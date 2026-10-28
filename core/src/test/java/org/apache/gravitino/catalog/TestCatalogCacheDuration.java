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
package org.apache.gravitino.catalog;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogCacheDuration {

  @Test
  void idleIntervalWhenNoDeadline() {
    long idleNanos = TimeUnit.HOURS.toNanos(1);
    Assertions.assertEquals(
        idleNanos, CatalogManager.cacheDurationNanos(idleNanos, Long.MAX_VALUE, 0L));
  }

  @Test
  void capsIdleIntervalByDeadline() {
    long now = 1_000_000L;
    long idleNanos = TimeUnit.HOURS.toNanos(1);
    long deadline = now + TimeUnit.MINUTES.toMillis(10);
    Assertions.assertEquals(
        TimeUnit.MINUTES.toNanos(10), CatalogManager.cacheDurationNanos(idleNanos, deadline, now));
  }

  @Test
  void expiresImmediatelyWhenDeadlineHasPassed() {
    long now = 5_000L;
    Assertions.assertEquals(
        0L, CatalogManager.cacheDurationNanos(TimeUnit.HOURS.toNanos(1), now, now));
  }
}
