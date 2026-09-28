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
package org.apache.gravitino.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SharedDbContainerService}'s pure parsing/liveness logic. Container
 * lifecycle itself (starting a container, readiness polling, stale-container removal) needs a
 * real Docker daemon and is exercised by the Core database Gradle lanes instead.
 */
public class TestSharedDbContainerService {

  @Test
  public void testParseHostPortSingleLine() {
    assertEquals(49153, SharedDbContainerService.parseHostPort("0.0.0.0:49153", "abc123"));
  }

  @Test
  public void testParseHostPortUsesFirstLineWhenMultipleAddressFamiliesPublished() {
    // `docker port` prints one line per address family when the daemon also publishes IPv6.
    assertEquals(
        49153,
        SharedDbContainerService.parseHostPort("0.0.0.0:49153\n[::]:49153", "abc123"));
  }

  @Test
  public void testParseHostPortTrimsWhitespace() {
    assertEquals(49153, SharedDbContainerService.parseHostPort("  0.0.0.0:49153  \n", "abc123"));
  }

  @Test
  public void testParseHostPortThrowsOnEmptyOutput() {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> SharedDbContainerService.parseHostPort("", "abc123"));
    assertTrue(e.getMessage().contains("abc123"));
  }

  @Test
  public void testParseHostPortThrowsOnNonNumericPort() {
    assertThrows(
        NumberFormatException.class,
        () -> SharedDbContainerService.parseHostPort("0.0.0.0:notaport", "abc123"));
  }

  @Test
  public void testSplitNonBlankLinesFiltersBlanksAndTrims() {
    List<String> lines = SharedDbContainerService.splitNonBlankLines("  a  \n\n  b\n   \nc");
    assertEquals(List.of("a", "b", "c"), lines);
  }

  @Test
  public void testSplitNonBlankLinesEmptyInputReturnsEmptyList() {
    assertTrue(SharedDbContainerService.splitNonBlankLines("").isEmpty());
    assertTrue(SharedDbContainerService.splitNonBlankLines("   \n  \n").isEmpty());
  }

  @Test
  public void testIsOwningProcessAliveTrueForCurrentProcess() {
    assertTrue(
        SharedDbContainerService.isOwningProcessAlive(
            String.valueOf(ProcessHandle.current().pid())));
  }

  @Test
  public void testIsOwningProcessAliveFalseForUnusedPid() {
    // PID 0 is reserved/invalid on essentially every OS Gradle runs on, and is never a real,
    // live user process - a safe stand-in for "definitely not alive" without depending on any
    // specific dead PID happening to be free at test time.
    assertFalse(SharedDbContainerService.isOwningProcessAlive("0"));
  }

  @Test
  public void testIsOwningProcessAliveFalseForUnparsableLabel() {
    // A missing/malformed PID label (e.g. a container from before this label existed) must bias
    // toward "not alive" so removeStaleContainers() reaps it rather than leaking it forever.
    assertFalse(SharedDbContainerService.isOwningProcessAlive(""));
    assertFalse(SharedDbContainerService.isOwningProcessAlive("not-a-pid"));
  }
}
