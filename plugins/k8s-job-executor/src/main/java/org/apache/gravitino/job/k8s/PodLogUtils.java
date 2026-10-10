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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** Reads the output of a job from the log of its pod. */
public final class PodLogUtils {

  private static final int READ_BUFFER_SIZE = 8192;

  // The largest array size that is safe to allocate on every JVM.
  private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

  private PodLogUtils() {}

  /**
   * Reads the last lines of a log stream, within its last bytes. Only the last bytes are kept in
   * memory while reading. If the kept bytes start in the middle of a line, that partial line is
   * dropped, unless it is the only line.
   *
   * @param log the log stream, which is read to its end
   * @param maxLines the maximum number of lines to return
   * @param maxBytes the maximum number of bytes to read from the tail of the log
   * @return the last lines of the log
   * @throws IOException if the log can't be read
   */
  public static List<String> readLastLines(InputStream log, int maxLines, int maxBytes)
      throws IOException {
    Preconditions.checkArgument(maxLines > 0, "maxLines must be positive");
    Preconditions.checkArgument(maxBytes > 0, "maxBytes must be positive");

    // Keep one more byte than the window, to tell whether the window starts at a line boundary.
    // The window is capped so that the buffer, of twice its size, fits in an array.
    int keep = (int) Math.min(maxBytes + 1L, MAX_BUFFER_SIZE / 2);
    int window = keep - 1;
    int maxCapacity = Math.max(2 * keep, READ_BUFFER_SIZE);
    // The buffer grows with the log, as most logs are much shorter than the window.
    byte[] tail = new byte[READ_BUFFER_SIZE];
    int length = 0;
    byte[] chunk = new byte[READ_BUFFER_SIZE];
    int read;
    while ((read = log.read(chunk)) >= 0) {
      if (length + read > tail.length && tail.length < maxCapacity) {
        long capacity = Math.max(2L * tail.length, (long) length + read);
        tail = Arrays.copyOf(tail, (int) Math.min(capacity, maxCapacity));
      }
      if (length + read > tail.length) {
        // Drop the head, keeping the last bytes of the buffer and the chunk.
        int kept = Math.max(keep - read, 0);
        System.arraycopy(tail, length - kept, tail, 0, kept);
        length = kept;
        if (read > keep) {
          System.arraycopy(chunk, read - keep, tail, 0, keep);
          length = keep;
          continue;
        }
      }
      System.arraycopy(chunk, 0, tail, length, read);
      length += read;
    }

    int start = 0;
    boolean startsAtLineBoundary = true;
    if (length > window) {
      start = length - window;
      startsAtLineBoundary = tail[start - 1] == '\n';
    }
    // Never start in the middle of a UTF-8 character. A '\n' is never part of one.
    while (start < length && (tail[start] & 0xC0) == 0x80) {
      start++;
    }
    String content = new String(tail, start, length - start, StandardCharsets.UTF_8);
    if (!startsAtLineBoundary) {
      int firstNewline = content.indexOf('\n');
      if (firstNewline >= 0 && firstNewline < content.length() - 1) {
        content = content.substring(firstNewline + 1);
      }
    }
    if (content.isEmpty()) {
      return ImmutableList.of();
    }

    List<String> lines = Arrays.asList(content.split("\\r?\\n", -1));
    // A trailing line terminator doesn't start another line.
    if (lines.get(lines.size() - 1).isEmpty()) {
      lines = lines.subList(0, lines.size() - 1);
    }
    // The window only holds the line terminator of a line that starts before it.
    if (!startsAtLineBoundary && lines.size() == 1 && lines.get(0).isEmpty()) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(lines.subList(Math.max(0, lines.size() - maxLines), lines.size()));
  }
}
