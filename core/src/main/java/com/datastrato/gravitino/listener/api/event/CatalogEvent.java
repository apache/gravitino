/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an abstract base class for events related to catalog operations. This class extends
 * {@link Event} to provide a more specific context involving operations on catalogs, such as
 * creation, deletion, or modification.
 */
@DeveloperApi
public abstract class CatalogEvent extends Event {
  /**
   * Constructs a new {@code CatalogEvent} with the specified user and catalog identifier.
   *
   * @param user The user responsible for triggering the catalog operation.
   * @param identifier The identifier of the catalog involved in the operation.
   */
  protected CatalogEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
