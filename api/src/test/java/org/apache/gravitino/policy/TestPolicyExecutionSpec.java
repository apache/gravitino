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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyExecutionSpec {

  @Test
  void emptyPeersReuseSingleton() {
    Assertions.assertSame(
        PolicyExecutionSpec.EMPTY, PolicyExecutionSpec.of(null, null, null, null, null));
    Assertions.assertSame(PolicyExecutionSpec.EMPTY, PolicyExecutionSpec.empty());
  }

  @Test
  void commitTriggerFalseIsRetainedWhenAlone() {
    PolicyExecutionSpec spec = PolicyExecutionSpec.of(null, false, null, null, null);
    Assertions.assertNotSame(PolicyExecutionSpec.EMPTY, spec);
    Assertions.assertEquals(Boolean.FALSE, spec.commitTrigger());
    Assertions.assertTrue(spec.hasAny());
  }

  @Test
  void toStringOmitsOptionValues() {
    PolicyExecutionSpec spec =
        PolicyExecutionSpec.of(
            "0 0 2 * * ?",
            true,
            1000L,
            ImmutableMap.of("password", "secret"),
            ImmutableMap.of("oauth_credential", "id:secret"));

    String text = spec.toString();
    Assertions.assertTrue(text.contains("jobOptionsKeys=[password]"));
    Assertions.assertTrue(text.contains("updaterOptionsKeys=[oauth_credential]"));
    Assertions.assertFalse(text.contains("secret"));
    Assertions.assertFalse(text.contains("id:secret"));
  }

  @Test
  void validateRejectsNegativeMinInterval() {
    PolicyExecutionSpec spec = PolicyExecutionSpec.of(null, null, -1L, null, null);
    IllegalArgumentException ex =
        Assertions.assertThrows(IllegalArgumentException.class, spec::validate);
    Assertions.assertTrue(ex.getMessage().contains("minIntervalMs"));
  }
}
