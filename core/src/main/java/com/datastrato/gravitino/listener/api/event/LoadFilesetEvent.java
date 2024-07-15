/*
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

package com.datastrato.gravitino.listener.api.event;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.annotation.DeveloperApi;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;

/** Represents an event that occurs when a fileset is loaded into the system. */
@DeveloperApi
public final class LoadFilesetEvent extends FilesetEvent {
  private final FilesetInfo loadedFilesetInfo;
  /**
   * Constructs a new {@code LoadFilesetEvent}.
   *
   * @param user The user who initiated the loading of the fileset.
   * @param identifier The unique identifier of the fileset being loaded.
   * @param loadedFilesetInfo The state of the fileset post-loading.
   */
  public LoadFilesetEvent(String user, NameIdentifier identifier, FilesetInfo loadedFilesetInfo) {
    super(user, identifier);
    this.loadedFilesetInfo = loadedFilesetInfo;
  }

  /**
   * Retrieves the state of the fileset as it was made available to the user after successful
   * loading.
   *
   * @return A {@link FilesetInfo} instance encapsulating the details of the fileset as loaded.
   */
  public FilesetInfo loadedFilesetInfo() {
    return loadedFilesetInfo;
  }
}
