/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to table operations. This class extends
 * {@link Event} to provide a more specific context
 * involving operations on tables, such as creation, deletion, or modification. It captures
 * essential information including the user performing the operation and the identifier of the table
 * being operated on.
 *
 * <p>Concrete implementations of this class should provide additional details pertinent to the
 * specific type of table operation being represented.
 */
@DeveloperApi
public abstract class TableEvent extends Event {
  /**
   * Constructs a new {@code TableEvent} with the specified user and table identifier.
   *
   * @param user The user responsible for triggering the table operation. This information is
   *     crucial for auditing and tracking purposes.
   * @param identifier The identifier of the table involved in the operation. This encapsulates
   *     details such as the metalake, catalog, schema, and table name.
   */
  protected TableEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
