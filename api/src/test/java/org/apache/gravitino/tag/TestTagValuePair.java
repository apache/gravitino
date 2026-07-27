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

package org.apache.gravitino.tag;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagValuePair {

  @Test
  void testValuedPair() {
    TagValuePair pair = TagValuePair.of("data_domain", "finance");

    Assertions.assertEquals("data_domain", pair.name());
    Assertions.assertEquals("finance", pair.value().get());
  }

  @Test
  void testValuelessPair() {
    TagValuePair pair = TagValuePair.valueless("pii");

    Assertions.assertEquals("pii", pair.name());
    Assertions.assertFalse(pair.value().isPresent());
  }

  @Test
  void testEqualsAndHashCode() {
    TagValuePair pair1 = TagValuePair.of("data_domain", "finance");
    TagValuePair pair2 = TagValuePair.of("data_domain", "finance");
    TagValuePair pair3 = TagValuePair.of("data_domain", "risk");
    TagValuePair pair4 = TagValuePair.valueless("data_domain");

    Assertions.assertEquals(pair1, pair2);
    Assertions.assertEquals(pair1.hashCode(), pair2.hashCode());
    Assertions.assertNotEquals(pair1, pair3);
    Assertions.assertNotEquals(pair1, pair4);
  }
}
