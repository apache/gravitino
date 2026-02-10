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

package org.apache.gravitino;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestVersion {

  @Test
  public void testParseReleaseCandidateVersions() {
    Assertions.assertArrayEquals(new int[] {1, 1, 0}, Version.parseVersionNumber("1.1.0rc0"));
    Assertions.assertArrayEquals(new int[] {1, 1, 0}, Version.parseVersionNumber("1.1.0rc1"));
    Assertions.assertArrayEquals(new int[] {1, 1, 0}, Version.parseVersionNumber("1.1.0rc1000"));
  }

  @Test
  public void testParseReleaseCandidateOutOfRange() {
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> Version.parseVersionNumber("1.1.0rc1001"));
  }
}
