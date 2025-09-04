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
package org.apache.gravitino.client;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Test;

public class TestGravitinoVersion {
  @Test
  void testParseVersionString() {
    // Test a valid the version string
    GravitinoVersion version = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    int[] versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test a valid the version string with hyphen separator
    version = new GravitinoVersion("2.5.3-1.0.1", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test a valid the version string with SNAPSHOT
    version = new GravitinoVersion("2.5.3-SNAPSHOT", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test a valid the version string with SNAPSHOT and hyphen separator
    version = new GravitinoVersion("2.5.3-SNAPSHOT-2.5.3-SNAPSHOT", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test a valid the version string with alpha
    version = new GravitinoVersion("2.5.3-alpha", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test a valid the version string with alpha and hyphen separator
    version = new GravitinoVersion("2.5.3-alpha-2.5.3-alpha", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test incubator version
    version = new GravitinoVersion("2.5.3-incubating", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test incubator version with hyphen separator
    version = new GravitinoVersion("2.5.3-incubating-2.5.3-incubating", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test incubator snapshot version
    version = new GravitinoVersion("2.5.3-incubating-SNAPSHOT", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    // Test incubator snapshot version with hyphen separator
    version =
        new GravitinoVersion(
            "2.5.3-incubating-SNAPSHOT-2.5.3-incubating-SNAPSHOT", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(2, versionNumber[0]);
    assertEquals(5, versionNumber[1]);
    assertEquals(3, versionNumber[2]);

    version =
        new GravitinoVersion("0.9.0-incubating-SNAPSHOT-0.8.0-SNAPSHOT", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(0, versionNumber[0]);
    assertEquals(9, versionNumber[1]);
    assertEquals(0, versionNumber[2]);

    version = new GravitinoVersion("1.0.0.dev0", "2023-01-01", "1234567");
    versionNumber = version.getVersionNumber();
    assertEquals(1, versionNumber[0]);
    assertEquals(0, versionNumber[1]);
    assertEquals(0, versionNumber[2]);

    // Test an invalid the version string with 2 part
    version = new GravitinoVersion("2.5", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with 2 part and hyphen separator
    version = new GravitinoVersion("2.5-2.5", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with 4 part
    version = new GravitinoVersion("2.5.7.6", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with 4 part and hyphen separator
    version = new GravitinoVersion("2.5.7.6-2.5.7.6", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with not number
    version = new GravitinoVersion("a.b.c", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with not number and hyphen separator
    version = new GravitinoVersion("a.b.c-a.b.c", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);
  }

  @Test
  void testVersionCompatibility() {
    GravitinoVersion version1 = new GravitinoVersion("2.6.3", "2023-01-01", "1234567");
    GravitinoVersion version2 = new GravitinoVersion("2.6.4", "2023-01-01", "1234567");
    assertTrue(version1.compatibleWithServerVersion(version2));

    version1 = new GravitinoVersion("2.6.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.6.3", "2023-01-01", "1234567");
    assertTrue(version1.compatibleWithServerVersion(version2));

    version1 = new GravitinoVersion("2.6.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.6.2", "2023-01-01", "1234567");
    assertTrue(version1.compatibleWithServerVersion(version2));

    version1 = new GravitinoVersion("2.6.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("3.5.4", "2023-01-01", "1234567");
    assertTrue(version1.compatibleWithServerVersion(version2));

    version1 = new GravitinoVersion("2.6.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    assertFalse(version1.compatibleWithServerVersion(version2));

    version1 = new GravitinoVersion("3.6.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.5.4", "2023-01-01", "1234567");
    assertFalse(version1.compatibleWithServerVersion(version2));
  }
}
