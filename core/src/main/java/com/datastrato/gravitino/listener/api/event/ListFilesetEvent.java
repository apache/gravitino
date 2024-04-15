/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered upon the successful listing of filesets within a system.
 */
@DeveloperApi
public final class ListFilesetEvent extends FilesetEvent {
  private final Namespace namespace;
  /**
   * Constructs a new {@code ListFilesetEvent}.
   *
   * @param user The user who initiated the listing of filesets.
   * @param namespace The namespace within which the filesets are listed. The namespace provides
   *     contextual information, identifying the scope and boundaries of the listing operation.
   */
  public ListFilesetEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.toString()));
    this.namespace = namespace;
  }

  /**
   * Retrieves the namespace associated with the failed listing event.
   *
   * @return The {@link Namespace} that was targeted during the failed listing operation.
   */
  public Namespace namespace() {
    return namespace;
  }
}
