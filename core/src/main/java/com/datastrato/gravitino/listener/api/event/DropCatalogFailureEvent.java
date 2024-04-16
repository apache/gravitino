/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to drop a catalog fails due to an
 * exception.
 */
@DeveloperApi
public final class DropCatalogFailureEvent extends CatalogFailureEvent {
  /**
   * Constructs a new {@code DropCatalogFailureEvent} instance, capturing detailed information about
   * the failed attempt to drop a catalog.
   *
   * @param user The user who initiated the drop catalog operation.
   * @param identifier The identifier of the catalog that the operation attempted to drop.
   * @param exception The exception that was thrown during the drop catalog operation, offering
   *     insights into what went wrong and why the operation failed.
   */
  public DropCatalogFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
