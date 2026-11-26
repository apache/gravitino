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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.semantic;

import static org.apache.gravitino.semantic.SemanticModelRevisionSelector.ACTIVE;
import static org.apache.gravitino.semantic.SemanticModelRevisionSelector.LATEST;
import static org.apache.gravitino.semantic.SemanticModelRevisionSelector.numbered;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TestSemanticModelRevisionSelector {
  @Test
  void testSelectorNumbers() {
    assertEquals(0, ACTIVE.getNumber());
    assertEquals(-1, LATEST.getNumber());
    assertEquals(1, numbered(1).getNumber());
    assertThrows(IllegalArgumentException.class, () -> numbered(ACTIVE.getNumber()));
    assertThrows(IllegalArgumentException.class, () -> numbered(LATEST.getNumber()));
  }
}
