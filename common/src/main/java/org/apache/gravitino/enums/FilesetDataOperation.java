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

package org.apache.gravitino.enums;

/** An enum class containing fileset data operations that supported. */
public enum FilesetDataOperation {
  /** This data operation means that create a new file. */
  CREATE,
  /** This data operation means that open a file. */
  OPEN,
  /** This data operation means that append some content into a file. */
  APPEND,
  /** This data operation means that rename a file or a directory. */
  RENAME,
  /** This data operation means that delete a file or a directory. */
  DELETE,
  /** This data operation means that get a file status from a file or a directory. */
  GET_FILE_STATUS,
  /** This data operation means that list file statuses under a directory. */
  LIST_STATUS,
  /** This data operation means that create a directory. */
  MKDIRS,
  /** This data operation means that get the default replication of a file system. */
  GET_DEFAULT_REPLICATION,
  /** This data operation means that get the default block size of a file system. */
  GET_DEFAULT_BLOCK_SIZE,
  /** This data operation means that set current working directory. */
  SET_WORKING_DIR,
  /** This data operation means that it is an unknown data operation. */
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
      throw new IllegalArgumentException("Unknown fileset data operation: " + operation, e);
    }
  }
}
