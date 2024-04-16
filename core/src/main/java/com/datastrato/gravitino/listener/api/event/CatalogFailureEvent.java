/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * An abstract class representing events that are triggered when a catalog operation fails due to an
 * exception. This class extends {@link FailureEvent} to provide a more specific context related to
 * catalog operations, encapsulating details about the user who initiated the operation, the
 * identifier of the catalog involved, and the exception that led to the failure.
 */
@DeveloperApi
public abstract class CatalogFailureEvent extends FailureEvent {
  /**
   * Constructs a new {@code CatalogFailureEvent} instance, capturing information about the failed
   * catalog operation.
   *
   * @param user The user associated with the failed catalog operation.
   * @param identifier The identifier of the catalog that was involved in the failed operation.
   * @param exception The exception that was thrown during the catalog operation, indicating the
   *     cause of the failure.
   */
  protected CatalogFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
