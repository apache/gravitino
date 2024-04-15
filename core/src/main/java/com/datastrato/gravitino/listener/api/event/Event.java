/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;
import javax.annotation.Nullable;

/**
 * The abstract base class for all events. It encapsulates common information such as the user who
 * generated the event and the identifier for the resource associated with the event. Subclasses
 * should provide specific details related to their individual event types.
 */
@DeveloperApi
public abstract class Event {
  private final String user;
  @Nullable private final NameIdentifier identifier;

  /**
   * Constructs an Event instance with the specified user and resource identifier details.
   *
   * @param user The user associated with this event. It provides context about who triggered the
   *     event.
   * @param identifier The resource identifier associated with this event. This may refer to various
   *     types of resources such as a metalake, catalog, schema, or table, etc.
   */
  protected Event(String user, NameIdentifier identifier) {
    this.user = user;
    this.identifier = identifier;
  }

  /**
   * Retrieves the user associated with this event.
   *
   * @return A string representing the user associated with this event.
   */
  public String user() {
    return user;
  }

  /**
   * Retrieves the resource identifier associated with this event.
   *
   * <p>For list operations within a namespace, the identifier is the identifier corresponds to that
   * namespace. For metalake list operation, identifier is null.
   *
   * @return A NameIdentifier object that represents the resource, like a metalake, catalog, schema,
   *     table, etc., associated with the event.
   */
  @Nullable
  public NameIdentifier identifier() {
    return identifier;
  }
}
