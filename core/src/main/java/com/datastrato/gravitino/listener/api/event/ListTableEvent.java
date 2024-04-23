/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered upon the successful list of tables within a namespace.
 *
 * <p>To optimize memory usage and avoid the potential overhead associated with storing a large
 * number of tables directly within the ListTableEvent, the actual tables listed are not maintained
 * in this event. This design decision helps in managing resource efficiency, especially in
 * environments with extensive table listings.
 */
@DeveloperApi
public final class ListTableEvent extends TableEvent {
  private final Namespace namespace;

  /**
   * Constructs an instance of {@code ListTableEvent}.
   *
   * @param user The username of the individual who initiated the table listing.
   * @param namespace The namespace from which tables were listed.
   */
  public ListTableEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which tables were listed.
   */
  public Namespace namespace() {
    return namespace;
  }
}
