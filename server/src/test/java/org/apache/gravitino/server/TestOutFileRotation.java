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
package org.apache.gravitino.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Verifies {@code rotate_out_file} in {@code bin/common.sh.template}. */
class TestOutFileRotation {

  @TempDir Path dir;

  @Test
  void testKeepsFivePreviousFilesByDefault() throws Exception {
    Path outFile = dir.resolve("gravitino-server.out");
    for (int run = 0; run < 7; run++) {
      write(outFile, "run-" + run);
      rotate(outFile, null);
    }

    assertFalse(Files.exists(outFile));
    for (int index = 1; index <= 5; index++) {
      assertEquals("run-" + (7 - index), read(outFile, index));
    }
    assertFalse(Files.exists(rotated(outFile, 6)));
  }

  @Test
  void testRemovesFilesBeyondCustomLimit() throws Exception {
    Path outFile = dir.resolve("gravitino-server.out");
    write(outFile, "current");
    for (int index = 1; index <= 6; index++) {
      write(rotated(outFile, index), "previous-" + index);
    }

    rotate(outFile, "2");

    assertFalse(Files.exists(outFile));
    assertEquals("current", read(outFile, 1));
    assertEquals("previous-1", read(outFile, 2));
    for (int index = 3; index <= 7; index++) {
      assertFalse(Files.exists(rotated(outFile, index)));
    }
  }

  @Test
  void testKeepsNoPreviousFileWhenLimitIsZero() throws Exception {
    Path outFile = dir.resolve("gravitino-server.out");
    write(outFile, "current");
    write(rotated(outFile, 1), "previous-1");

    rotate(outFile, "0");

    assertFalse(Files.exists(outFile));
    assertFalse(Files.exists(rotated(outFile, 1)));
  }

  @Test
  void testFallsBackToDefaultLimitWhenLimitIsInvalid() throws Exception {
    Path outFile = dir.resolve("gravitino-server.out");
    for (int index = 1; index <= 5; index++) {
      write(rotated(outFile, index), "previous-" + index);
    }
    write(outFile, "current");

    rotate(outFile, "abc");

    assertEquals("current", read(outFile, 1));
    assertEquals("previous-4", read(outFile, 5));
    assertFalse(Files.exists(rotated(outFile, 6)));
  }

  @Test
  void testDoesNothingWithoutOutFile() throws Exception {
    Path outFile = dir.resolve("gravitino-server.out");

    rotate(outFile, null);

    assertFalse(Files.exists(outFile));
    assertFalse(Files.exists(rotated(outFile, 1)));
  }

  private void rotate(Path outFile, @Nullable String keep) throws Exception {
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash", "-c", ". \"${COMMON_SH}\" > /dev/null && rotate_out_file \"${OUT_FILE}\"");
    builder.redirectErrorStream(true);
    Map<String, String> env = builder.environment();
    env.put(
        "COMMON_SH",
        TestLog4j2RollingConfiguration.rootDir().resolve("bin/common.sh.template").toString());
    env.put("OUT_FILE", outFile.toString());
    env.put("GRAVITINO_HOME", dir.toString());
    env.put("GRAVITINO_CONF_DIR", dir.resolve("conf").toString());
    env.put("GRAVITINO_LOG_DIR", dir.toString());
    env.put("GRAVITINO_VERSION", "test");
    env.remove("GRAVITINO_OUT_FILE_KEEP");
    if (keep != null) {
      env.put("GRAVITINO_OUT_FILE_KEEP", keep);
    }

    Process process = builder.start();
    String output;
    try (InputStream input = process.getInputStream()) {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      input.transferTo(buffer);
      output = buffer.toString(StandardCharsets.UTF_8);
    }
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "rotate_out_file timed out");
    assertEquals(0, process.exitValue(), "rotate_out_file failed: " + output);
  }

  private static Path rotated(Path outFile, int index) {
    return outFile.resolveSibling(outFile.getFileName() + "." + index);
  }

  private static void write(Path file, String content) throws IOException {
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
  }

  private static String read(Path outFile, int index) throws IOException {
    return new String(Files.readAllBytes(rotated(outFile, index)), StandardCharsets.UTF_8);
  }
}
