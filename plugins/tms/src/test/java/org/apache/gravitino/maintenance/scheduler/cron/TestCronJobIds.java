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
package org.apache.gravitino.maintenance.scheduler.cron;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCronJobIds {

  @Test
  void testForTableAndParseRoundTrip() {
    String id = CronJobIds.forTable(10L, 20L);
    Assertions.assertEquals("table:10:20", id);
    Optional<CronJobIds.TablePolicyIds> parsed = CronJobIds.parseTable(id);
    Assertions.assertTrue(parsed.isPresent());
    Assertions.assertEquals(10L, parsed.get().tableId());
    Assertions.assertEquals(20L, parsed.get().policyId());
  }

  @Test
  void testParseRejectsInvalid() {
    Assertions.assertTrue(CronJobIds.parseTable(null).isEmpty());
    Assertions.assertTrue(CronJobIds.parseTable("batch:1:2").isEmpty());
    Assertions.assertTrue(CronJobIds.parseTable("table:").isEmpty());
    Assertions.assertTrue(CronJobIds.parseTable("table:1").isEmpty());
    Assertions.assertTrue(CronJobIds.parseTable("table:a:2").isEmpty());
  }
}
