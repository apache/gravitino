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
  /** Creates a new file. */
  CREATE,
  /** Opens a file. */
  OPEN,
  /** Opens a file and writes to it. */
  OPEN_AND_WRITE,
  /** Opens a file and appends to it. */
  OPEN_AND_APPEND,
  /** Appends some content into a file. */
  APPEND,
  /** Renames a file or a directory. */
  RENAME,
  /** Deletes a file or a directory. */
  DELETE,
  /** Gets a file status from a file or a directory. */
  GET_FILE_STATUS,
  /** Lists file statuses under a directory. */
  LIST_STATUS,
  /** Creates a directory. */
  MKDIRS,
  /** Gets the default replication of a file system. */
  GET_DEFAULT_REPLICATION,
  /** Gets the default block size of a file system. */
  GET_DEFAULT_BLOCK_SIZE,
  /** Sets current working directory. */
  SET_WORKING_DIR,
  /** Gets the current working directory. */
  EXISTS,
  /** Gets the created time of a file. */
  CREATED_TIME,
  /** Gets the modified time of a file. */
  MODIFIED_TIME,
  /** Copies a file. */
  COPY_FILE,
  /** Gets the content of a file. */
  CAT_FILE,
  /** Copies a remote file to local. */
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
