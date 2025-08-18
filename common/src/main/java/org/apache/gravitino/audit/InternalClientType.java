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

/** An enum class containing internal client type that supported. */
public enum InternalClientType {
  /**
   * The client type is `org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem` which in
   * the filesystem-hadoop3 module.
   */
  HADOOP_GVFS,
  /**
   * The client type is `gravitino.filesystem.gvfs.GravitinoVirtualFileSystem` which in the
   * client-python module.
   */
  PYTHON_GVFS,
  /** The client type is unknown. */
  UNKNOWN;

  /**
   * Check if the client type is valid.
   *
   * @param clientType the client type
   * @return true if the client type is valid, false otherwise
   */
  public static boolean checkValid(String clientType) {
    if (clientType == null) {
      return false;
    }
    try {
      InternalClientType.valueOf(clientType);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
