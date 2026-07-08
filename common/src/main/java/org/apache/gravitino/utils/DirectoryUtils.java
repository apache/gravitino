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

package org.apache.gravitino.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/** Utilities for working with local directories. */
public class DirectoryUtils {

  private DirectoryUtils() {}

  /**
   * Ensures that the given directory exists, creating it and any missing parent directories if
   * necessary.
   *
   * <p>Unlike {@code File#exists()} followed by {@code File#mkdirs()}, this method is safe against
   * concurrent creation of the same directory: it succeeds if the directory already exists or is
   * created concurrently by another thread or process.
   *
   * @param dir the directory to create
   * @throws IOException if the directory cannot be created, or if the path exists but is not a
   *     directory
   */
  public static void ensureDirectory(File dir) throws IOException {
    try {
      Files.createDirectories(dir.toPath());
    } catch (IOException e) {
      throw new IOException(
          String.format("Failed to create directory %s", dir.getAbsolutePath()), e);
    }
  }
}
