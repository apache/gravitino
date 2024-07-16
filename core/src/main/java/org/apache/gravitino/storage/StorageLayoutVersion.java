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

package org.apache.gravitino.storage;

import lombok.Getter;

/** The storage layer version of the entity store. */
@Getter
public enum StorageLayoutVersion {
  V1("v1");

  private final String version;

  StorageLayoutVersion(String version) {
    this.version = version;
  }

  public static StorageLayoutVersion fromString(String version) {
    for (StorageLayoutVersion v : StorageLayoutVersion.values()) {
      if (v.version.equals(version)) {
        return v;
      }
    }
    throw new StorageLayoutException(
        "Unknown storage version, maybe the data is broken, please check the storage directory.");
  }
}
