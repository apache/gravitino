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
package org.apache.gravitino.file;

/**
 * Represents metadata about a single file or directory within a fileset.
 *
 * <p>Implementations of this interface provide access to the basic attributes of a fileset entry,
 * including its name, type (file vs. directory), size, last-modified timestamp, and its logical
 * path within the enclosing fileset.
 */
public interface FileInfo {

  /** @return The filename or directory name of file object. */
  String name();

  /** @return Whether this is a directory (true). */
  boolean isDir();

  /** @return The file size in bytes (0 if directory). */
  long size();

  /** @return The last modification time as an Instant. */
  long lastModified();

  /** @return The full path of the file or directory within the fileset. */
  String path();
}
