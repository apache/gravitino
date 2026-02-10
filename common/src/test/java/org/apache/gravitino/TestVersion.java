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

import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> Version.parseVersionNumber("1.1.0rc"));
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> Version.parseVersionNumber("1.1.0rc-1"));
  }

  @Test
  public void testParseSnapshotVersion() {
    Assertions.assertArrayEquals(new int[] {1, 1, 0}, Version.parseVersionNumber("1.1.0-SNAPSHOT"));
    Assertions.assertArrayEquals(new int[] {2, 0, 1}, Version.parseVersionNumber("2.0.1-beta"));
    Assertions.assertArrayEquals(new int[] {3, 2, 4}, Version.parseVersionNumber("3.2.4-foo.bar"));
  }

  @Test
  public void testParseAlphaVersion() {
    Assertions.assertArrayEquals(new int[] {1, 1, 0}, Version.parseVersionNumber("1.1.0.alpha"));
    Assertions.assertArrayEquals(new int[] {0, 9, 9}, Version.parseVersionNumber("0.9.9.alpha1"));
  }
}
