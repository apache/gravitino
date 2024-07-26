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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.file.FilesetDataOperationCtx;

/** Represents an event that occurs when getting a fileset context. */
@DeveloperApi
public final class GetFilesetContextEvent extends FilesetEvent {
  private final FilesetDataOperationCtx ctx;

  /**
   * Constructs a new {@code GetFilesetContextEvent}, recording the attempt to get a fileset
   * context.
   *
   * @param user The user who initiated the get fileset context operation.
   * @param identifier The identifier of the fileset context that was attempted to be got.
   * @param ctx The data operation context to get the fileset context.
   */
  public GetFilesetContextEvent(
      String user, NameIdentifier identifier, FilesetDataOperationCtx ctx) {
    super(user, identifier);
    this.ctx = ctx;
  }

  public FilesetDataOperationCtx dataOperationContext() {
    return ctx;
  }
}
