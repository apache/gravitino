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
package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyExecutionFields {

  @Test
  void testValidateAcceptsTimeAndCommitTriggers() {
    Assertions.assertDoesNotThrow(
        () ->
            PolicyExecutionFields.validate(
                new TimeTrigger("0 0 2 * * ?"),
                CommitTrigger.INSTANCE,
                100L,
                ImmutableMap.of("uri", "http://irc"),
                ImmutableMap.of("metalake", "ml")));
  }

  @Test
  void testCommitTriggerPresenceIsValid() {
    Assertions.assertDoesNotThrow(
        () ->
            PolicyExecutionFields.validate(
                null, new CommitTrigger(), null, Collections.emptyMap(), Collections.emptyMap()));
  }

  @Test
  void testTimeTriggerRequiresCron() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PolicyExecutionFields.validate(
                new TimeTrigger(null), null, null, Collections.emptyMap(), Collections.emptyMap()));
  }
}
