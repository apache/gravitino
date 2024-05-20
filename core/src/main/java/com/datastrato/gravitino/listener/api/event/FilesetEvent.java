/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to fileset operations. Extending {@link
 * com.datastrato.gravitino.listener.api.event.Event}, this class narrows the focus to operations
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
