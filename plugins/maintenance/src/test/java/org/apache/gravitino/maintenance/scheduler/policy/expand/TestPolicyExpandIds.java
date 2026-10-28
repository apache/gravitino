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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestPolicyExpandIds {

  @Test
  void testForPolicyAndParseRoundTrip() {
    Assertions.assertEquals("policy:42", PolicyExpandIds.forPolicy(42L));
    Assertions.assertEquals(42L, PolicyExpandIds.parsePolicy("policy:42").orElseThrow());
  }

  @Test
  void testParseRejectsInvalid() {
    Assertions.assertTrue(PolicyExpandIds.parsePolicy(null).isEmpty());
    Assertions.assertTrue(PolicyExpandIds.parsePolicy("42").isEmpty());
    Assertions.assertTrue(PolicyExpandIds.parsePolicy("policy:").isEmpty());
    Assertions.assertTrue(PolicyExpandIds.parsePolicy("policy:a").isEmpty());
    Assertions.assertTrue(PolicyExpandIds.parsePolicy("policy:1:2").isEmpty());
    Assertions.assertTrue(PolicyExpandIds.parsePolicy("table:1").isEmpty());
  }
}
