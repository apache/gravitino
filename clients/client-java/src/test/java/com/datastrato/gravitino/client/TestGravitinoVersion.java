/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
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

    // Test a valid the version string with SNAPSHOT
    version = new GravitinoVersion("2.5.3-SNAPSHOT", "2023-01-01", "1234567");
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

    // Test an invalid the version string with 2 part
    version = new GravitinoVersion("2.5", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with 4 part
    version = new GravitinoVersion("2.5.7.6", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);

    // Test an invalid the version string with not number
    version = new GravitinoVersion("a.b.c", "2023-01-01", "1234567");
    assertThrows(GravitinoRuntimeException.class, version::getVersionNumber);
  }

  @Test
  void testVersionCompare() {
    GravitinoVersion version1 = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    // test equal
    GravitinoVersion version2 = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    assertEquals(0, version1.compareTo(version2));

    // test less than
    version1 = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.5.4", "2023-01-01", "1234567");
    assertTrue(version1.compareTo(version2) < 0);

    // test greater than
    version1 = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.5.2", "2023-01-01", "1234567");
    assertTrue(version1.compareTo(version2) > 0);

    // test equal with suffix
    version1 = new GravitinoVersion("2.5.3", "2023-01-01", "1234567");
    version2 = new GravitinoVersion("2.5.3-SNAPSHOT", "2023-01-01", "1234567");
    assertEquals(0, version1.compareTo(version2));
  }
  @Test
    void testVersionEquals() {
      //test equal
      GravitinoVersion version1 = new GravitinoVersion("3.1.3", "2022-01-01", "1234567");
      GravitinoVersion version2 = new GravitinoVersion("3.1.3", "2022-01-01", "1234567");
      assertTrue(version1.equals(version2));
  
      //test not equal
      version1 = new GravitinoVersion("3.1.4", "2022-01-01", "1234567");
      version2 = new GravitinoVersion("3.1.2", "2022-01-01", "1234567");
      assertFalse(version1.equals(version2));
  
      //test equal with suffix
      version1 = new GravitinoVersion("3.1.6-BETA", "2022-01-01", "1234567");
      version2 = new GravitinoVersion("3.1.6", "2022-01-01", "1234567");
      assertTrue(version1.equals(version2));
    }
    
}
