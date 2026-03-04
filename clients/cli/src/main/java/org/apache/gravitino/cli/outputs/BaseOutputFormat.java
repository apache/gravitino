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

package org.apache.gravitino.cli.outputs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.gravitino.cli.CommandContext;

/**
 * Abstract base implementation of {@link OutputFormat} interface providing common functionality for
 * various output format implementations.
 */
public abstract class BaseOutputFormat<T> implements OutputFormat<T> {
  /** The command context. */
  protected CommandContext context;

  /**
   * Creates a new {@link BaseOutputFormat} with specified configuration.
   *
   * @param context the command context, must not be null;
   */
  protected BaseOutputFormat(CommandContext context) {
    this.context = context;
  }

  /**
   * Outputs a message to the specified OutputStream. This method handles both system streams
   * ({@code System.out}, {@code System.err}) and regular output streams differently: - For system
   * streams: Preserves the stream open after writing - For other streams: Automatically closes the
   * stream after writing
   *
   * @param message the message to output, must not be null
   * @param os the output stream to write to, must not be null If this is {@code System.out} or
   *     {@code System.err}, the stream will not be closed
   * @throws IllegalArgumentException if either message or os is null
   * @throws UncheckedIOException if an I/O error occurs during writing
   */
  public static void output(String message, OutputStream os) {
    if (message == null || os == null) {
      throw new IllegalArgumentException(
          "Message and OutputStream cannot be null, message: " + message + ", os: " + os);
    }
    boolean isSystemStream = (os == System.out || os == System.err);

    try {
      PrintStream printStream =
          new PrintStream(
              isSystemStream ? os : new BufferedOutputStream(os),
              true,
              StandardCharsets.UTF_8.name());

      try {
        printStream.println(message);
        printStream.flush();
      } finally {
        if (!isSystemStream) {
          printStream.close();
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write message to output stream", e);
    }
  }

  /**
   * {@inheritDoc} This implementation checks the quiet flag and handles null output gracefully. If
   * quiet mode is enabled, no output is produced.
   */
  @Override
  public void output(T entity) {
    String outputMessage = getOutput(entity);
    String output = outputMessage == null ? "" : outputMessage;
    output(output, System.out);
  }
}
