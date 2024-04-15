/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list filesets within a namespace fails.
 */
@DeveloperApi
public final class ListFilesetFailureEvent extends FilesetFailureEvent {
  private final Namespace namespace;

  /**
   * Constructs a new {@code ListFilesetFailureEvent}.
   *
   * @param user The username of the individual associated with the failed fileset listing
   *     operation.
   * @param namespace The namespace in which the fileset listing was attempted.
   * @param exception The exception encountered during the fileset listing attempt, which serves as
   *     an indicator of the issues that caused the failure.
   */
  public ListFilesetFailureEvent(String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.toString()), exception);
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
