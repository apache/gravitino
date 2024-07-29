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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to fileset operations. Extending {@link
 * org.apache.gravitino.listener.api.event.Event}, this class narrows the focus to operations
 * performed on filesets, such as creation, deletion, or modification. It captures vital information
 * including the user performing the operation and the identifier of the fileset being manipulated.
 *
 * <p>Concrete implementations of this class are expected to provide additional specifics relevant
 * to the particular type of fileset operation being represented, enriching the contextual
 * understanding of each event.
 */
@DeveloperApi
public abstract class FilesetEvent extends Event {
  /**
   * Constructs a new {@code FilesetEvent} with the specified user and fileset identifier.
   *
   * @param user The user responsible for initiating the fileset operation. This information is
   *     critical for auditing and tracking the origin of actions.
   * @param identifier The identifier of the fileset involved in the operation. This includes
   *     details essential for pinpointing the specific fileset affected by the operation.
   */
  protected FilesetEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
