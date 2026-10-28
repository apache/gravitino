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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCommitJobIds {

  @Test
  void testForTableAndParseRoundTrip() {
    Assertions.assertEquals("table:99", CommitJobIds.forTable(99L));
    Assertions.assertEquals(99L, CommitJobIds.parseTable("table:99").orElseThrow());
  }

  @Test
  void testParseRejectsCronStyleAndInvalid() {
    Assertions.assertTrue(CommitJobIds.parseTable(null).isEmpty());
    Assertions.assertTrue(CommitJobIds.parseTable("99").isEmpty());
    Assertions.assertTrue(CommitJobIds.parseTable("table:").isEmpty());
    Assertions.assertTrue(CommitJobIds.parseTable("table:a").isEmpty());
    Assertions.assertTrue(CommitJobIds.parseTable("table:1;policy:2").isEmpty());
    Assertions.assertTrue(CommitJobIds.parseTable("table:1:policy:2").isEmpty());
    Assertions.assertTrue(CommitJobIds.parseTable("policy:1").isEmpty());
  }
}
