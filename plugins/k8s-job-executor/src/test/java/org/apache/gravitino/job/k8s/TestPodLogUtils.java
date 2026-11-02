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
package org.apache.gravitino.job.k8s;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPodLogUtils {

  @Test
  public void testEmpty() throws IOException {
    Assertions.assertEquals(ImmutableList.of(), read("", 10, 100));
  }

  @Test
  public void testWithinLimits() throws IOException {
    Assertions.assertEquals(ImmutableList.of("a", "b"), read("a\nb\n", 10, 100));
    Assertions.assertEquals(ImmutableList.of("a", "", "b"), read("a\r\n\r\nb", 10, 100));
  }

  @Test
  public void testMaxLines() throws IOException {
    Assertions.assertEquals(ImmutableList.of("b", "c"), read("a\nb\nc\n", 2, 100));
  }

  @Test
  public void testMaxBytesDropsPartialLine() throws IOException {
    // The last 6 bytes are "bb\ncc\n", "bb" is a partial line.
    Assertions.assertEquals(ImmutableList.of("cc"), read("aaa\nbbbb\ncc\n", 10, 6));
    // The last 8 bytes are "bbbb\ncc\n", which start right after a newline.
    Assertions.assertEquals(ImmutableList.of("bbbb", "cc"), read("aaa\nbbbb\ncc\n", 10, 8));
  }

  @Test
  public void testMaxBytesKeepsSingleLongLine() throws IOException {
    Assertions.assertEquals(ImmutableList.of("6789"), read("0123456789\n", 10, 5));
  }

  @Test
  public void testMaxBytesSkipsPartialCharacter() throws IOException {
    // "é" is 2 bytes in UTF-8, the window starts at its second byte.
    Assertions.assertEquals(ImmutableList.of("xy"), read("éxy", 10, 3));
  }

  @Test
  public void testWindowOnlyHoldsLineTerminator() throws IOException {
    Assertions.assertEquals(ImmutableList.of(), read("abc\n", 10, 1));
    Assertions.assertEquals(ImmutableList.of(), read("abc\r\n", 10, 1));
    Assertions.assertEquals(ImmutableList.of(), read("abc\r\n", 10, 2));
    // An empty last line is a line of its own.
    Assertions.assertEquals(ImmutableList.of(""), read("abc\n\n", 10, 1));
  }

  @Test
  public void testHugeMaxBytes() throws IOException {
    for (int maxBytes : new int[] {(1 << 30) - 1, 1 << 30, Integer.MAX_VALUE}) {
      Assertions.assertEquals(ImmutableList.of("a", "b"), read("a\nb\n", 10, maxBytes));
    }
  }

  @Test
  public void testLongLog() throws IOException {
    String log =
        IntStream.range(0, 100_000).mapToObj(i -> "line-" + i).collect(Collectors.joining("\n"));
    Assertions.assertEquals(
        ImmutableList.of("line-99998", "line-99999"),
        read(log, 1000, "line-99998\nline-99999".length()));
    Assertions.assertEquals(ImmutableList.of("line-99999"), read(log, 1, 100_000));
    List<String> lines = read(log, 1000, 1 << 20);
    Assertions.assertEquals(1000, lines.size());
    Assertions.assertEquals("line-99000", lines.get(0));
  }

  private static List<String> read(String log, int maxLines, int maxBytes) throws IOException {
    return PodLogUtils.readLastLines(
        new ByteArrayInputStream(log.getBytes(StandardCharsets.UTF_8)), maxLines, maxBytes);
  }
}
