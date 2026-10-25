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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link PolicyExpandCrons}. */
class TestPolicyExpandCrons {

  @Test
  void testNextExecutionUsesUtcIndependentOfJvmDefaultZone() {
    // Daily at 01:00 — wall clock depends on the CronSchedule zone, not the JVM default.
    String dailyAtOne = "0 0 1 * * ?";
    Instant after = Instant.parse("2026-06-15T00:30:00Z");

    TimeZone previous = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("America/Los_Angeles")));
      Instant fromUs = PolicyExpandCrons.nextExecution(dailyAtOne, after);

      TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai")));
      Instant fromCn = PolicyExpandCrons.nextExecution(dailyAtOne, after);

      Assertions.assertEquals(fromUs, fromCn);
      Assertions.assertEquals(Instant.parse("2026-06-15T01:00:00Z"), fromUs);
      Assertions.assertEquals(ZoneOffset.UTC, PolicyExpandCrons.SCHEDULER_ZONE);
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  @Test
  void testInvalidCronRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PolicyExpandCrons.nextExecution("not-a-cron"));
  }
}
