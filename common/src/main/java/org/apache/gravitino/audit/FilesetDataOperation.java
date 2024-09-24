/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.audit;

/** An enum class containing fileset data operations that supported. */
public enum FilesetDataOperation {
  /** Create a new file. */
  CREATE,
  /** Open a file. */
  OPEN,
  /** Append some content into a file. */
  APPEND,
  /** Rename a file or a directory. */
  RENAME,
  /** Delete a file or a directory. */
  DELETE,
  /** Get a file status from a file or a directory. */
  GET_FILE_STATUS,
  /** List file statuses under a directory. */
  LIST_STATUS,
  /** Create a directory. */
  MKDIRS,
  /** Get the default replication of a file system. */
  GET_DEFAULT_REPLICATION,
  /** Get the default block size of a file system. */
  GET_DEFAULT_BLOCK_SIZE,
  /** Set current working directory. */
  SET_WORKING_DIR,
  /** Get the current working directory. */
  EXISTS,
  /** Get the created time of a file. */
  CREATED_TIME,
  /** Get the modified time of a file. */
  MODIFIED_TIME,
  /** Copy a file. */
  COPY_FILE,
  /** Get the content of a file. */
  CAT_FILE,
  /** Copy a remote file to local. */
  GET_FILE,
  /** Unknown data operation. */
  UNKNOWN;

  /**
   * Check if the operation is valid.
   *
   * @param operation the operation to check
   * @return true if the operation is valid, false otherwise
   */
  public static boolean checkValid(String operation) {
    try {
      FilesetDataOperation.valueOf(operation);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
